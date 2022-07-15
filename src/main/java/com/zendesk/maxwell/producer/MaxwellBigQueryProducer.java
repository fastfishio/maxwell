package com.zendesk.maxwell.producer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.services.bigquery.model.JsonObject;
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
import com.zendesk.maxwell.producer.AbstractAsyncProducer.CallbackCompleter;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.BqToBqStorageSchemaConverter;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Phaser;
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
  Map<RowMap, CallbackCompleter> rowCallbackPairs;
  private MaxwellContext context;

  private Counter succeededMessageCount;
  private Counter failedMessageCount;
  private Meter succeededMessageMeter;
  private Meter failedMessageMeter;

  private static final int MAX_RETRY_COUNT = 2;
  private final ImmutableList<Code> RETRIABLE_ERROR_CODES = ImmutableList.of(Code.INTERNAL, Code.ABORTED,
      Code.CANCELLED);

  public BigQueryCallback(MaxwellBigQueryProducerWorker parent,
      Map<RowMap, CallbackCompleter> rowCallbackPairs,
      Counter producedMessageCount, Counter failedMessageCount,
      Meter succeededMessageMeter, Meter failedMessageMeter,
      MaxwellContext context) {
    this.parent = parent;
    this.rowCallbackPairs = rowCallbackPairs;
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
        for (Map.Entry<RowMap, CallbackCompleter> pair : rowCallbackPairs.entrySet()) {
          RowMap r = pair.getKey();
          CallbackCompleter cc = pair.getValue();
          LOGGER.debug("-> {}\n" +
              " {}\n",
              r.toJSON(), r.getNextPosition());
          cc.markCompleted();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void onFailure(Throwable t) {
    this.failedMessageCount.inc();
    this.failedMessageMeter.mark();

    LOGGER.error(t.getLocalizedMessage());

    Status status = Status.fromThrowable(t);

    if (!this.context.getConfig().ignoreProducerError) {
      StorageException storageException = Exceptions.toStorageException(t);
      this.parent.setError((storageException != null) ? storageException : new RuntimeException(t));
      context.terminate();
      return;
    }

    for (Map.Entry<RowMap, CallbackCompleter> pair : rowCallbackPairs.entrySet()) {
      pair.getValue().markCompleted();
    }

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

  class MaxwellBigQueryProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
    static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducerWorker.class);

    private final ArrayBlockingQueue<RowMap> queue;
    private StoppableTaskState taskState;
    private Thread thread;
    private final Object lock = new Object();

    @GuardedBy("lock")
    private RuntimeException error = null;
    private JsonStreamWriter streamWriter;

    public MaxwellBigQueryProducerWorker(MaxwellContext context,
        ArrayBlockingQueue<RowMap> queue, String bigQueryProjectId,
        String bigQueryDataset, String bigQueryTable) throws IOException {
      super(context);
      this.queue = queue;
      Metrics metrics = context.getMetrics();
      this.taskState = new StoppableTaskState("MaxwellBigQueryProducerWorker");
    }

    public Object getLock() {
      return lock;
    }

    public RuntimeException getError() {
      return error;
    }

    public void setError(RuntimeException error) {
      this.error = error;
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
      synchronized (this.lock) {
        if (this.error != null) {
          throw this.error;
        }
      }
    }

    @Override
    public void awaitStop(Long timeout) throws TimeoutException {
      taskState.awaitStop(thread, timeout);
    }

    @Override
    public void run() {
      this.thread = Thread.currentThread();
      ArrayList<RowMap> rows = new ArrayList<RowMap>(10);
      while (true) {
        try {
          RowMap row = queue.take();
          if (!taskState.isRunning()) {
            taskState.stopped();
            return;
          }
          // this.push(row);
          rows.add(row);
          if(rows.size() >= 10) {
              this.multiPush(rows);
              rows.clear();
          }
        } catch (Exception e) {
          taskState.stopped();
          context.terminate(e);
          return;
        }
      }
    }

    @Override
    public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
    }

    @Override
    public void sendMultiAsync(Map<RowMap, CallbackCompleter> rowCallbackPairs) throws Exception {
      synchronized (this.lock) {
        if (this.error != null) {
          throw this.error;
        }
      }
      JSONArray jsonArr = new JSONArray();
      for (Map.Entry<RowMap, CallbackCompleter> pair : rowCallbackPairs.entrySet()) {
        RowMap r = pair.getKey();
        CallbackCompleter c = pair.getValue();
        JSONObject record = new JSONObject(r.toJSON(outputConfig));
        // convert json and array fields to String
        covertJSONObjectFieldsToString(record);
        jsonArr.put(record);
      }
      ApiFuture<AppendRowsResponse> future = streamWriter.append(jsonArr);
      ApiFutures.addCallback(
          future, new BigQueryCallback(this, rowCallbackPairs,
              this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter,
              this.context),
          MoreExecutors.directExecutor());
    }
  
}