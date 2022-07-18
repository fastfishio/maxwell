package com.zendesk.maxwell.producer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.BqToBqStorageSchemaConverter;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellBigQueryProducer extends AbstractProducer {

  private final ArrayBlockingQueue<RowMap> queue;
  private final MaxwellBigQueryProducerWorker worker;

  public MaxwellBigQueryProducer(MaxwellContext context,String bigQueryProjectId,
  String bigQueryDataset, String bigQueryTable)
      throws IOException {
    super(context);
    this.queue = new ArrayBlockingQueue<>(100);
    this.worker = new MaxwellBigQueryProducerWorker(context, this.queue, bigQueryProjectId, bigQueryDataset, bigQueryTable);

    TableName table = TableName.of(bigQueryProjectId, bigQueryDataset, bigQueryTable);
    try {
      this.worker.initialize(table);
    } catch (DescriptorValidationException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Thread thread = new Thread(this.worker, "maxwell-bigquery-worker");
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  public void push(RowMap r) throws Exception {
    this.queue.put(r);
  }

  @Override
  public StoppableTask getStoppableTask() {
    return this.worker;
  }

}

class MaxwellBigQueryProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducerWorker.class);

  private final ArrayBlockingQueue<RowMap> queue;
  private StoppableTaskState taskState;
  private Thread thread;
  private MaxwellContext context;

  public MaxwellBigQueryProducerWorker(MaxwellContext context,
      ArrayBlockingQueue<RowMap> queue,String bigQueryProjectId,
      String bigQueryDataset, String bigQueryTable) throws IOException {
        super(context);
    this.queue = queue;
    this.context = context;
    Metrics metrics = context.getMetrics();
    this.taskState = new StoppableTaskState("MaxwellBigQueryProducerWorker");
  }

  private JsonStreamWriter streamWriter;

  public void initialize(TableName tName)
      throws DescriptorValidationException, IOException, InterruptedException {

    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(tName.getProject()).build().getService();
    Table table = bigquery.getTable(tName.getDataset(), tName.getTable());
    Schema schema = table.getDefinition().getSchema();
    TableSchema tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema);
    streamWriter = JsonStreamWriter.newBuilder(tName.toString(), tableSchema).build();
  }

  @Override
  public void requestStop() throws Exception {
    taskState.requestStop();
    streamWriter.close();
  }

  @Override
  public void awaitStop(Long timeout) throws TimeoutException {
    taskState.awaitStop(thread, timeout);
  }

  @Override
  public void run() {
    this.thread = Thread.currentThread();
    ArrayList<RowMap> rows = new ArrayList<RowMap>(100);
    while (true) {
      // pull rows from queue, and add them to arraylist
      // once we have 100 elements, we will push in bulk via BQ API
      try {
        RowMap row = queue.take();
        if (!taskState.isRunning()) {
          taskState.stopped();
          return;
        }
        if(row.shouldOutput(outputConfig))
          rows.add(row);
        if(rows.size() >= 100) {
            this.sendMany(rows);
            rows.clear();
        }
      } catch (Exception e) {
        taskState.stopped();
        context.terminate(e);
        return;
      }
    }
  }
  private void covertJSONObjectFieldsToString(JSONObject record) {
    if (this.context.getConfig().outputConfig.includesPrimaryKeys) {
      record.put("primary_key", record.get("primary_key").toString());
    }
    String data = record.has("data") == true ? record.get("data").toString() : null;
    record.put("data", data);
    String old = record.has("old") == true ? record.get("old").toString() : null;
    record.put("old", old);
  }

  public void sendMany(ArrayList<RowMap> rows) throws Exception {
    JSONArray jsonArr = new JSONArray();
    for(RowMap r : rows) {
        JSONObject record = new JSONObject(r.toJSON(outputConfig));
        LOGGER.debug("maxwell incoming log -> " + r.toJSON(outputConfig));
        //convert json and array fields to String
        covertJSONObjectFieldsToString(record);
        jsonArr.put(record);
    }
    int tryCount = 0;
    int maxTries = 3;
    while(true){
      ApiFuture<AppendRowsResponse> future = streamWriter.append(jsonArr);
      // will throw TimeoutException if takes more than 60 seconds, crashing maxwell and restarting
      AppendRowsResponse res = future.get(60, TimeUnit.SECONDS);
      if(res.hasError()) {
        if (++tryCount == maxTries){
          List<String> rowsString = new ArrayList<String>();
          for(RowMap r : rows)
            rowsString.add(r.toJSON(outputConfig));
          
          LOGGER.error("bq insertion error in this batch -> " + String.join("\n", rowsString));
          if(!this.context.getConfig().ignoreProducerError){
            throw new Exception("bq insertion error -> " + res.getError().toString());
          }
          else{
            this.context.setPosition(rows.get(rows.size() - 1));
            break;
          }
        }
        continue;
      }
      this.context.setPosition(rows.get(rows.size() - 1));
      break;
    }
    
  }

  @Override
  public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
    // TODO Auto-generated method stub
  }

}