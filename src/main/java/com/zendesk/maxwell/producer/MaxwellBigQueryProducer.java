package com.zendesk.maxwell.producer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import javax.annotation.concurrent.GuardedBy;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryCallback implements ApiFutureCallback<AppendRowsResponse> {
  public final Logger LOGGER = LoggerFactory.getLogger(BigQueryCallback.class);

  private final MaxwellBigQueryProducerWorker parent;
  private final AbstractAsyncProducer.CallbackCompleter cc;
  private final Position position;
  private MaxwellContext context;
  AppendContext appendContext;

  private Counter succeededMessageCount;
  private Counter failedMessageCount;
  private Meter succeededMessageMeter;
  private Meter failedMessageMeter;


  public BigQueryCallback(MaxwellBigQueryProducerWorker parent,
      AppendContext appendContext,
      AbstractAsyncProducer.CallbackCompleter cc,
      Position position,
      Counter producedMessageCount, Counter failedMessageCount,
      Meter succeededMessageMeter, Meter failedMessageMeter,
      MaxwellContext context) {
    this.parent = parent;
    this.appendContext = appendContext;
    this.cc = cc;
    this.position = position;
    this.succeededMessageCount = producedMessageCount;
    this.failedMessageCount = failedMessageCount;
    this.succeededMessageMeter = succeededMessageMeter;
    this.failedMessageMeter = failedMessageMeter;
    this.context = context;
  }

  @Override
  public void onSuccess(AppendRowsResponse response) {
    this.succeededMessageCount.inc();
    this.succeededMessageMeter.mark();

    if (LOGGER.isDebugEnabled()) {
      try {
        LOGGER.debug("-> {}\n" +
            " {}\n",
            this.appendContext.r.toJSON(), this.position);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    cc.markCompleted();
  }

  @Override
  public void onFailure(Throwable t) {
    this.failedMessageCount.inc();
    this.failedMessageMeter.mark();

    LOGGER.error(t.getClass().getSimpleName() + " @ " + position);
    LOGGER.error(t.getLocalizedMessage());

    LOGGER.error("bq insertion error ->" + appendContext.data.toString());

    if (!this.context.getConfig().ignoreProducerError) {
      this.context.terminate(new RuntimeException(t));
      return;
    }
    
    cc.markCompleted();
  }
}

public class MaxwellBigQueryProducer extends AbstractProducer {

  private final ArrayBlockingQueue<RowMap> queue;
  private final MaxwellBigQueryProducerWorker worker;

  public MaxwellBigQueryProducer(MaxwellContext context, String bigQueryProjectId,
      String bigQueryDataset, String bigQueryTable)
      throws IOException {
    super(context);
    this.queue = new ArrayBlockingQueue<>(100);
    this.worker = new MaxwellBigQueryProducerWorker(context, this.queue, bigQueryProjectId, bigQueryDataset,
        bigQueryTable);

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
}

class AppendContext {
  JSONArray data;
  int retryCount = 0;
  RowMap r = null;

  AppendContext(JSONArray data, int retryCount, RowMap r) {
    this.data = data;
    this.retryCount = retryCount;
    this.r = r;
  }
}

class MaxwellBigQueryProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducerWorker.class);

  private final ArrayBlockingQueue<RowMap> queue;
  private StoppableTaskState taskState;
  private Thread thread;
  private JsonStreamWriter streamWriter;

  public MaxwellBigQueryProducerWorker(MaxwellContext context,
      ArrayBlockingQueue<RowMap> queue, String bigQueryProjectId,
      String bigQueryDataset, String bigQueryTable) throws IOException {
    super(context);
    this.queue = queue;
    Metrics metrics = context.getMetrics();
    this.taskState = new StoppableTaskState("MaxwellBigQueryProducerWorker");
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
    while (true) {
      try {
        RowMap row = queue.take();
        if (!taskState.isRunning()) {
          taskState.stopped();
          return;
        }
        this.push(row);
      } catch (Exception e) {
        taskState.stopped();
        context.terminate(e);
        return;
      }
    }
  }

  @Override
  public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
    JSONArray jsonArr = new JSONArray();
    JSONObject record = new JSONObject(r.toJSON(outputConfig));
    //convert json and array fields to String
    covertJSONObjectFieldsToString(record);
    jsonArr.put(record);
    AppendContext appendContext = new AppendContext(jsonArr, 0, r);

    ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);
    ApiFutures.addCallback(
        future, new BigQueryCallback(this, appendContext, cc, r.getNextPosition(),
            this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter,
            this.context),
        MoreExecutors.directExecutor());
  }
}
