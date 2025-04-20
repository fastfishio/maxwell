package com.zendesk.maxwell.producer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
// Keep other Google Cloud imports: BigQuery, BigQueryOptions, Schema, Table, storage.v1.*
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
import com.google.common.util.concurrent.MoreExecutors; // Removed later
import com.google.common.util.concurrent.ThreadFactoryBuilder; // For naming threads
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
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
  private final Position position;
  private MaxwellContext context;
  AppendContext appendContext;

  private Counter succeededMessageCount;
  private Counter failedMessageCount;
  private Meter succeededMessageMeter;
  private Meter failedMessageMeter;

  private static final int MAX_RETRY_COUNT = 2;
  private final ImmutableList<Code> RETRIABLE_ERROR_CODES = ImmutableList.of(Code.INTERNAL, Code.ABORTED,
      Code.CANCELLED);

  public BigQueryCallback(MaxwellBigQueryProducerWorker parent, 
      AppendContext appendContext,
      Counter producedMessageCount, Counter failedMessageCount,
      Meter succeededMessageMeter, Meter failedMessageMeter,
      MaxwellContext context) {
    this.parent = parent;
    this.appendContext = appendContext;
    this.position = appendContext.position;
    this.succeededMessageCount = producedMessageCount;
    this.failedMessageCount = failedMessageCount;
    this.succeededMessageMeter = succeededMessageMeter;
    this.failedMessageMeter = failedMessageMeter;
    this.context = context;
  }

  @Override
  public void onSuccess(AppendRowsResponse response) {
    for (int i = 0; i < appendContext.callbacks.size(); i++) {
        this.succeededMessageCount.inc();
        this.succeededMessageMeter.mark();
        AbstractAsyncProducer.CallbackCompleter cc = (AbstractAsyncProducer.CallbackCompleter) appendContext.callbacks.get(i);
        cc.markCompleted();

        if (LOGGER.isDebugEnabled()) {
          try {
            LOGGER.debug("Worker {} -> {}\n", parent.getWorkerId(), this.position); 
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
    }
  }

  @Override
  public void onFailure(Throwable t) {
    for (int i = 0; i < appendContext.callbacks.size(); i++) {
        this.failedMessageCount.inc();
        this.failedMessageMeter.mark();
    }

    LOGGER.error("Worker {} " + t.getClass().getSimpleName() + " @ " + position, parent.getWorkerId());
    LOGGER.error("Worker {} " + t.getLocalizedMessage(), parent.getWorkerId());

    Status status = Status.fromThrowable(t);
    if (appendContext.retryCount < MAX_RETRY_COUNT
        && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
      appendContext.retryCount++;
      try {
        this.parent.attemptBatch(appendContext);
        return;
      } catch (Exception e) {
        System.out.format("Worker {} Failed to retry append: %s\n", parent.getWorkerId(), e);
      }
    }

    synchronized (this.parent.getLock()) {
      if (this.parent.getError() == null && !this.context.getConfig().ignoreProducerError) {
        StorageException storageException = Exceptions.toStorageException(t);
        this.parent.setError((storageException != null) ? storageException : new RuntimeException(t));
        context.terminate();
        return;
      }
    }
    // got an error, but we are ingoring producer error
    for (int i = 0; i < appendContext.callbacks.size(); i++) {
        AbstractAsyncProducer.CallbackCompleter cc = (AbstractAsyncProducer.CallbackCompleter) appendContext.callbacks.get(i);
        cc.markCompleted();
    }
  }
}

public class MaxwellBigQueryProducer extends AbstractProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducer.class);

  private final ArrayBlockingQueue<RowMap> queue;
  private final List<MaxwellBigQueryProducerWorker> workers;
  private final ExecutorService workerExecutor;
  private final ExecutorService callbackExecutor;

  public MaxwellBigQueryProducer(MaxwellContext context, String bigQueryProjectId,
      String bigQueryDataset, String bigQueryTable) 
      throws IOException {
    super(context);
    int numWorkers = Runtime.getRuntime().availableProcessors();
    this.queue = new ArrayBlockingQueue<>(numWorkers * MaxwellBigQueryProducerWorker.BATCH_SIZE);

    ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("bq-worker-%d").setDaemon(true).build();
    this.workerExecutor = Executors.newFixedThreadPool(Math.max(1, numWorkers), workerThreadFactory);

    ThreadFactory callbackThreadFactory = new ThreadFactoryBuilder().setNameFormat("bq-callback-%d").setDaemon(true).build();
    this.callbackExecutor = Executors.newCachedThreadPool(callbackThreadFactory);

    this.workers = new ArrayList<>(Math.max(1, numWorkers));
    TableName tableName = TableName.of(bigQueryProjectId, bigQueryDataset, bigQueryTable);
    startWorkers(context, tableName);
  }
  
  private void startWorkers(MaxwellContext context, TableName tableName) throws IOException {
    int numWorkers = this.workers.size();
    TableSchema tableSchema = getTableSchema(tableName);
     // Create and start workers
    for (int i = 0; i < Math.max(1, numWorkers); i++) {
       try {
            MaxwellBigQueryProducerWorker worker = new MaxwellBigQueryProducerWorker(
                context,
                this.queue,
                this.callbackExecutor, // Pass callback executor
                i // Pass worker ID
            );
            worker.initialize(tableName, tableSchema);
            this.workers.add(worker);
            this.workerExecutor.submit(worker);
       } catch (DescriptorValidationException | IOException | InterruptedException e) {
           LOGGER.error("Failed to initialize MaxwellBigQueryProducer worker {}: {}", i, e.getMessage(), e);
           // Don't try to shutdown executors, just throw
           throw new IOException("Failed to initialize worker " + i, e);
       }
    }
    LOGGER.info("Submitted {} workers to executor.", this.workers.size());
  }

  private TableSchema getTableSchema(TableName tName) throws IOException {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(tName.getProject()).build().getService();
    Table table = bigquery.getTable(tName.getDataset(), tName.getTable());
    Schema schema = table.getDefinition().getSchema();
    TableSchema tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema);
    return tableSchema;
  }

  @Override
  public void push(RowMap r) throws Exception {
    this.queue.put(r);
  }
}
class MaxwellBigQueryProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducerWorker.class);
  public static final int BATCH_SIZE = 100;


  private final ArrayBlockingQueue<RowMap> queue;
  private StoppableTaskState taskState;
  private Thread thread;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private RuntimeException error = null; 
  private JsonStreamWriter streamWriter;
  private final ExecutorService callbackExecutor;
  private final int workerId;
  private AppendContext appendContext;

  public MaxwellBigQueryProducerWorker(MaxwellContext context,
      ArrayBlockingQueue<RowMap> queue,
      ExecutorService callbackExecutor,
      int workerId) throws IOException {
    super(context);
    this.queue = queue;
    this.callbackExecutor = callbackExecutor; 
    this.workerId = workerId;
    Metrics metrics = context.getMetrics();
    this.taskState = new StoppableTaskState("MaxwellBigQueryProducerWorker-" + workerId); // Keep taskState init
  }

  public Object getLock() {
    return lock;
  }

  public int getWorkerId() {
    return workerId;
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


  public void initialize(TableName tName, TableSchema tableSchema)
      throws DescriptorValidationException, IOException, InterruptedException {
    this.streamWriter = JsonStreamWriter.newBuilder(tName.toString(), tableSchema).build();
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
    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
      }

      if(this.appendContext == null) {
        this.appendContext = new AppendContext();
      }
    }

    JSONObject record = new JSONObject(r.toJSON(outputConfig));
    covertJSONObjectFieldsToString(record);
    this.appendContext.addRow(r, record, cc);

	// TODO: also trigger batch if it has been a while since batch was created?
    if(this.appendContext.callbacks.size() >= BATCH_SIZE) {
        synchronized (this.getLock()) {
            this.attemptBatch(this.appendContext);
            this.appendContext = null;
        }
    }
  }

  public void attemptBatch(AppendContext appendContext) throws DescriptorValidationException, IOException {
    ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.data);

    ApiFutures.addCallback(
        future, new BigQueryCallback(this, appendContext,
            this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter,
            this.context),
        this.callbackExecutor
    );

  }

}


class AppendContext {
  JSONArray data;
  int retryCount = 0;
  int records = 0;
  public ArrayList<AbstractAsyncProducer.CallbackCompleter> callbacks;
  Position position;

  AppendContext() {
    this.data = new JSONArray();
    this.retryCount = 0;
    this.records = 0;
    this.callbacks = new ArrayList<AbstractAsyncProducer.CallbackCompleter>();
  }

  public void addRow(RowMap r, JSONObject record, AbstractAsyncProducer.CallbackCompleter cc) {
    this.data.put(record);
    this.callbacks.add(cc);
    if(this.position == null) {
        this.position = r.getNextPosition();
    }
  }
}

