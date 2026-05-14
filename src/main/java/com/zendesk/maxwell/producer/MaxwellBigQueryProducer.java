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
import com.google.common.util.concurrent.ThreadFactoryBuilder; // For naming threads
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.BqToBqStorageSchemaConverter;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
      String bigQueryDataset, String bigQueryTable, int bigqueryThreads,
      String bigQueryDdlProjectId, String bigQueryDdlDataset, String bigQueryDdlTable,
      String bigQueryDdlInstance, int bigQueryDdlBatchSize)
      throws IOException {
    super(context);
    bigqueryThreads = Math.max(1, bigqueryThreads);
    this.queue = new ArrayBlockingQueue<>(bigqueryThreads * MaxwellBigQueryProducerWorker.BATCH_SIZE);

    boolean hasDdlDataset = bigQueryDdlDataset != null && !bigQueryDdlDataset.isEmpty();
    boolean hasDdlTable = bigQueryDdlTable != null && !bigQueryDdlTable.isEmpty();
    boolean hasDdlInstance = bigQueryDdlInstance != null && !bigQueryDdlInstance.isEmpty();
    boolean ddlSinkEnabled = hasDdlDataset && hasDdlTable;
    if ( (hasDdlDataset != hasDdlTable) || (hasDdlInstance && !ddlSinkEnabled) ) {
      throw new IOException(
          "bigquery_ddl_dataset and bigquery_ddl_table must both be set to enable the BigQuery DDL metadata sink; "
              + "bigquery_ddl_instance is optional (omit when the DDL table has no instance column)");
    }
    int ddlBatchSize = Math.max(1, bigQueryDdlBatchSize);

    ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("bq-worker-%d").setDaemon(true).build();
    this.workerExecutor = Executors.newFixedThreadPool(bigqueryThreads, workerThreadFactory);

    ThreadFactory callbackThreadFactory = new ThreadFactoryBuilder().setNameFormat("bq-callback-%d").setDaemon(true).build();
    this.callbackExecutor = Executors.newCachedThreadPool(callbackThreadFactory);

    this.workers = new ArrayList<>(bigqueryThreads);
    TableName tableName = TableName.of(bigQueryProjectId, bigQueryDataset, bigQueryTable);
    TableName ddlTableName = null;
    TableSchema ddlTableSchema = null;
    if ( ddlSinkEnabled ) {
      String ddlProject = (bigQueryDdlProjectId != null && !bigQueryDdlProjectId.isEmpty())
          ? bigQueryDdlProjectId
          : bigQueryProjectId;
      ddlTableName = TableName.of(ddlProject, bigQueryDdlDataset, bigQueryDdlTable);
      ddlTableSchema = getDdlMetadataTableSchema(ddlTableName);
      LOGGER.info("[bq-producer] DDL metadata sink enabled: {} (ddl_batch_size={})", ddlTableName, ddlBatchSize);
    }
    startWorkers(context, tableName, ddlTableName, ddlTableSchema, ddlSinkEnabled, bigQueryDdlInstance,
        ddlBatchSize, bigqueryThreads);
  }

  private void startWorkers(MaxwellContext context, TableName tableName, TableName ddlTableName,
      TableSchema ddlTableSchema, boolean ddlSinkEnabled, String ddlInstance, int ddlBatchSize,
      int bigqueryThreads) throws IOException {
    TableSchema tableSchema = getTableSchema(tableName);
    for (int i = 0; i < bigqueryThreads; i++) {
      try {
        MaxwellBigQueryProducerWorker worker = new MaxwellBigQueryProducerWorker(
            context,
            this.queue,
            this.callbackExecutor,
            i,
            ddlSinkEnabled,
            ddlInstance,
            ddlBatchSize);
        worker.initialize(tableName, tableSchema, ddlTableName, ddlTableSchema);
        this.workers.add(worker);
        this.workerExecutor.submit(worker);
      } catch (DescriptorValidationException | IOException | InterruptedException e) {
        LOGGER.error("Failed to initialize MaxwellBigQueryProducer worker {}: {}", i, e.getMessage(), e);
        throw new IOException("Failed to initialize worker " + i, e);
      }
    }
    LOGGER.info("Submitted {} workers to executor.", this.workers.size());
  }

  private static TableSchema getTableSchema(TableName tName) throws IOException {
    return getTableSchema(tName, Collections.emptySet());
  }

  private static TableSchema getDdlMetadataTableSchema(TableName tName) throws IOException {
    return getTableSchema(tName, new HashSet<>(Arrays.asList("bq_created_at", "info")));
  }

  private static TableSchema getTableSchema(TableName tName, Set<String> skipExtraColumns) throws IOException {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(tName.getProject()).build().getService();
    Table table = bigquery.getTable(tName.getDataset(), tName.getTable());
    Schema schema = table.getDefinition().getSchema();
    Set<String> skip = new HashSet<>(skipExtraColumns);
    skip.add("bq_inserted_at");
    List<com.google.cloud.bigquery.Field> filteredFields = schema.getFields().stream()
      .filter(field -> !skip.contains(field.getName()))
      .collect(Collectors.toList());
    Schema filteredSchema = Schema.of(filteredFields);
    return BqToBqStorageSchemaConverter.convertTableSchema(filteredSchema);
  }

  @Override
  public void push(RowMap r) throws Exception {
    this.queue.put(r);
  }
}
class MaxwellBigQueryProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBigQueryProducerWorker.class);
  public static final int BATCH_SIZE = 100;
  // checked approximately, leave a buffer
  public static final long MAX_MESSAGE_SIZE_BYTES = 5_000_000;

  private static final class ColumnTransformation {
    final String jsonPath;
    final String transform;

    ColumnTransformation(String jsonPath, String transform) {
      this.jsonPath = jsonPath;
      this.transform = transform;
    }
  }

  private static final Map<String, Map<String, Map<String, List<ColumnTransformation>>>> COLUMN_TRANSFORMATIONS;

  static {
    String envValue = System.getenv("column_transformations");
    COLUMN_TRANSFORMATIONS = parseColumnTransformations(envValue);
    if (!COLUMN_TRANSFORMATIONS.isEmpty()) {
      LOGGER.info("Loaded column transformations: {}", envValue);
    }
  }

  private static Map<String, Map<String, Map<String, List<ColumnTransformation>>>> parseColumnTransformations(String envValue) {
    if (envValue == null || envValue.isEmpty()) {
      return Collections.emptyMap();
    }

    String cleaned = envValue.replaceAll("\\s+", "");
    String[] entries = cleaned.split(",");
    Map<String, Map<String, Map<String, List<ColumnTransformation>>>> transformations = new HashMap<>();
    java.util.Set<String> seen = new java.util.HashSet<>();

    for (String entry : entries) {
      if (entry.isEmpty()) {
        continue;
      }

      int ampIdx = entry.indexOf('&');
      if (ampIdx < 1 || ampIdx == entry.length() - 1) {
        throw new IllegalArgumentException(
          "Invalid column_transformations entry (missing '&' or empty transform/target): '" + entry + "'");
      }

      String transform = entry.substring(0, ampIdx);
      String target = entry.substring(ampIdx + 1);

      String[] dbTableCol = target.split("\\.", 3);
      if (dbTableCol.length != 3) {
        throw new IllegalArgumentException(
          "Invalid column_transformations target (expected database.table.column[:jsonpath]): '" + target + "'");
      }

      String database = dbTableCol[0];
      String table = dbTableCol[1];
      String columnPart = dbTableCol[2];

      if (!database.matches("[a-zA-Z0-9_]+") || !table.matches("[a-zA-Z0-9_]+")) {
        throw new IllegalArgumentException(
          "Invalid database or table name (alphanumeric and underscores only): '" + target + "'");
      }

      String columnName;
      String jsonPath = null;
      int colonIdx = columnPart.indexOf(':');
      if (colonIdx >= 0) {
        columnName = columnPart.substring(0, colonIdx);
        jsonPath = columnPart.substring(colonIdx + 1);
        if (jsonPath.isEmpty() || !jsonPath.startsWith("$")) {
          throw new IllegalArgumentException(
            "Invalid json path (must start with '$'): '" + jsonPath + "' in entry '" + entry + "'");
        }
      } else {
        columnName = columnPart;
      }

      if (!columnName.matches("[a-zA-Z0-9_]+")) {
        throw new IllegalArgumentException(
          "Invalid column name (alphanumeric and underscores only): '" + columnName + "' in entry '" + entry + "'");
      }

      String fullKey = database + "." + table + "." + columnName + (jsonPath != null ? ":" + jsonPath : "");
      if (!seen.add(fullKey)) {
        throw new IllegalArgumentException(
          "Duplicate column_transformations entry for: '" + fullKey + "'");
      }

      List<ColumnTransformation> columnTransformations = transformations
        .computeIfAbsent(database, db -> new HashMap<>())
        .computeIfAbsent(table, t -> new HashMap<>())
        .computeIfAbsent(columnName, c -> new ArrayList<>());

      if (jsonPath == null && !columnTransformations.isEmpty()) {
        throw new IllegalArgumentException(
          "Cannot mix whole-column and json-path transformations for: '" + database + "." + table + "." + columnName + "'");
      }
      if (!columnTransformations.isEmpty() && columnTransformations.get(0).jsonPath == null) {
        throw new IllegalArgumentException(
          "Cannot mix whole-column and json-path transformations for: '" + database + "." + table + "." + columnName + "'");
      }

      columnTransformations.add(new ColumnTransformation(jsonPath, transform));
    }

    return Collections.unmodifiableMap(transformations);
  }



  private final ArrayBlockingQueue<RowMap> queue;
  private StoppableTaskState taskState;
  private Thread thread;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private RuntimeException error = null;
  private JsonStreamWriter streamWriter;
  private JsonStreamWriter ddlStreamWriter;
  private final ScheduledExecutorService scheduledExecutor;
  private final ExecutorService callbackExecutor;
  private final int workerId;
  private AppendContext appendContext;
  private AppendContext ddlAppendContext;
  private final boolean ddlSinkEnabled;
  private final String ddlInstance;
  private final int ddlBatchSize;

  public MaxwellBigQueryProducerWorker(MaxwellContext context,
      ArrayBlockingQueue<RowMap> queue,
      ExecutorService callbackExecutor,
      int workerId,
      boolean ddlSinkEnabled,
      String ddlInstance,
      int ddlBatchSize) throws IOException {
    super(context, "bigquery-worker-" + workerId);
    this.queue = queue;
    this.callbackExecutor = callbackExecutor;
    this.workerId = workerId;
    this.ddlSinkEnabled = ddlSinkEnabled;
    this.ddlInstance = ddlInstance;
    this.ddlBatchSize = ddlBatchSize;
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("bq-batch-scheduler-" + workerId).setDaemon(true).build());
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


  public void initialize(TableName mainTable, TableSchema mainSchema, TableName ddlTable, TableSchema ddlSchema)
      throws DescriptorValidationException, IOException, InterruptedException {
    this.streamWriter = JsonStreamWriter.newBuilder(mainTable.toString(), mainSchema).build();
    if ( ddlTable != null && ddlSchema != null ) {
      this.ddlStreamWriter = JsonStreamWriter.newBuilder(ddlTable.toString(), ddlSchema).build();
    }
  }

  @Override
  protected boolean shouldEnqueueRow(RowMap r) {
    if ( ddlSinkEnabled && r instanceof DDLMap ) {
      MaxwellOutputConfig tmp = new MaxwellOutputConfig();
      tmp.outputDDL = true;
      return r.shouldOutput(tmp);
    }
    return super.shouldEnqueueRow(r);
  }

  @Override
  public void requestStop() throws Exception {
    taskState.requestStop();
    streamWriter.close();
    if ( ddlStreamWriter != null ) {
      ddlStreamWriter.close();
    }
    scheduledExecutor.shutdown();
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
    if ( ddlSinkEnabled && r instanceof DDLMap ) {
      sendDdlAsync((DDLMap) r, cc);
      return;
    }

    JSONObject record = new JSONObject(r.toJSON(outputConfig));
    applyColumnTransformations(r, record);
    covertJSONObjectFieldsToString(record);

    int recordSize = AppendContext.getJsonByteSize(record);
    if (recordSize >= 9 * 1024 * 1024) {
        LOGGER.error("Worker {} skipping oversized record: {} bytes for table {}.{}, position {}",
            this.workerId, recordSize, r.getDatabase(), r.getTable(), r.getNextPosition());
        cc.markCompleted();
        return;
    }

    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
      }

      if(this.appendContext == null) {
        this.appendContext = new AppendContext(this.streamWriter, false);
        this.scheduleFlush(this.appendContext);
      }
    }

    this.appendContext.addRow(r, record, cc);

    if(this.appendContext.callbacks.size() >= BATCH_SIZE
       || this.appendContext.getApproximateSize() >= MAX_MESSAGE_SIZE_BYTES) {
        synchronized (this.getLock()) {
            this.attemptBatch(this.appendContext);
            this.appendContext = null;
        }
    }
  }

  private void sendDdlAsync(DDLMap ddl, CallbackCompleter cc) throws Exception {
    if ( ddlStreamWriter == null ) {
      cc.markCompleted();
      return;
    }
    Object typeObj = ddl.getChangeMap().get("type");
    String changeType = typeObj != null ? typeObj.toString() : "unknown";
    JSONObject record = new JSONObject();
    if ( ddlInstance != null && !ddlInstance.isEmpty() ) {
      record.put("instance", ddlInstance);
    }
    record.put("type", changeType);
    record.put("statement", ddl.getSql() != null ? ddl.getSql() : JSONObject.NULL);

    int recordSize = AppendContext.getJsonByteSize(record);
    if (recordSize >= 9 * 1024 * 1024) {
      LOGGER.error("Worker {} skipping oversized DDL record: {} bytes, position {}",
          this.workerId, recordSize, ddl.getNextPosition());
      cc.markCompleted();
      return;
    }

    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
      }
      if (this.ddlAppendContext == null) {
        this.ddlAppendContext = new AppendContext(this.ddlStreamWriter, true);
        this.scheduleFlush(this.ddlAppendContext);
      }
    }

    this.ddlAppendContext.addRow(ddl, record, cc);

    if (this.ddlAppendContext.callbacks.size() >= this.ddlBatchSize
        || this.ddlAppendContext.getApproximateSize() >= MAX_MESSAGE_SIZE_BYTES) {
      synchronized (this.getLock()) {
        this.attemptBatch(this.ddlAppendContext);
        this.ddlAppendContext = null;
      }
    }
  }

  public void attemptBatch(AppendContext appendContext) throws DescriptorValidationException, IOException {
    if(appendContext.scheduledTask != null && !appendContext.scheduledTask.isDone()) {
      appendContext.scheduledTask.cancel(false);
    }
    ApiFuture<AppendRowsResponse> future = appendContext.streamWriter.append(appendContext.data);

    ApiFutures.addCallback(
        future, new BigQueryCallback(this, appendContext,
            this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter,
            this.context),
        this.callbackExecutor
    );
  }


  private void scheduleFlush(final AppendContext ctx) {
    if (ctx.scheduledTask != null && !ctx.scheduledTask.isDone()) {
      ctx.scheduledTask.cancel(false);
    }
    ctx.scheduledTask = this.scheduledExecutor.schedule(() -> {
      try {
        synchronized (this.getLock()) {
          if ( ctx.forDdl ) {
            if ( this.ddlAppendContext != ctx ) {
              return;
            }
          } else if ( this.appendContext != ctx ) {
            return;
          }
          if ( ctx.callbacks.isEmpty() ) {
            return;
          }
          this.attemptBatch(ctx);
          if ( ctx.forDdl ) {
            this.ddlAppendContext = null;
          } else {
            this.appendContext = null;
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error sending scheduled bigquery batch message");
        e.printStackTrace();
      }
    }, 1, TimeUnit.MINUTES);
  }

  private void applyColumnTransformations(RowMap row, JSONObject record) {
    if (COLUMN_TRANSFORMATIONS.isEmpty()) {
      return;
    }

    Map<String, Map<String, List<ColumnTransformation>>> databaseConfig = COLUMN_TRANSFORMATIONS.get(row.getDatabase());
    if (databaseConfig == null) {
      return;
    }

    Map<String, List<ColumnTransformation>> tableConfig = databaseConfig.get(row.getTable());
    if (tableConfig == null || tableConfig.isEmpty()) {
      return;
    }

    applyTransformations(record.opt("data"), tableConfig);
    applyTransformations(record.opt("old"), tableConfig);
  }

  private void applyTransformations(Object obj, Map<String, List<ColumnTransformation>> tableConfig) {
    if (!(obj instanceof JSONObject)) {
      return;
    }

    JSONObject target = (JSONObject) obj;
    for (Map.Entry<String, List<ColumnTransformation>> entry : tableConfig.entrySet()) {
      String columnName = entry.getKey();
      if (!target.has(columnName)) {
        continue;
      }

      for (ColumnTransformation t : entry.getValue()) {
        if (t.jsonPath == null) {
          target.put(columnName, transformValue(t.transform, target.get(columnName)));
        } else {
          Object columnValue = target.get(columnName);
          if (columnValue instanceof JSONObject) {
            applyJsonPathTransform((JSONObject) columnValue, t.jsonPath, t.transform);
          }
        }
      }
    }
  }

  private void applyJsonPathTransform(JSONObject root, String jsonPath, String transform) {
    String[] keys = jsonPath.split("\\.");
    if (keys.length < 2 || !"$".equals(keys[0])) {
      LOGGER.warn("Invalid json path: {}", jsonPath);
      return;
    }

    JSONObject current = root;
    for (int i = 1; i < keys.length - 1; i++) {
      Object next = current.opt(keys[i]);
      if (!(next instanceof JSONObject)) {
        return;
      }
      current = (JSONObject) next;
    }

    String leafKey = keys[keys.length - 1];
    if (current.has(leafKey)) {
      current.put(leafKey, transformValue(transform, current.get(leafKey)));
    }
  }

  private Object transformValue(String transformType, Object originalValue) {
    try {
      if (transformType == null) {
        return originalValue;
      }

      String[] parts = transformType.split(":", 2);
      String type = parts[0].trim().toLowerCase();
      String info = parts.length > 1 ? parts[1].trim() : null;

      switch (type) {
        case "clear":
          return transformClear(originalValue, info);
        case "mask":
          return transformMask(originalValue, info);
        default:
          LOGGER.warn("Unknown column transformation type: {}", transformType);
          return originalValue;
      }
    } catch (Exception e) {
      LOGGER.error("Error transforming value with transform '{}': {}", transformType, e.getMessage());
      return originalValue;
    }
  }

  private Object transformClear(Object originalValue, String info) {
    return JSONObject.NULL;
  }

  private Object transformMask(Object originalValue, String info) {
    if (originalValue == null || originalValue == JSONObject.NULL || info == null || !(originalValue instanceof String)) {
      return originalValue;
    }

    String[] params = info.split(":", 2);
    if (params.length != 2) {
      LOGGER.warn("mask requires two parameters X and Y, got: {}", info);
      return originalValue;
    }

    int keepFirst = Integer.parseInt(params[0].trim());
    int keepLast = Integer.parseInt(params[1].trim());
    String value = (String) originalValue;

    if (keepFirst + keepLast >= value.length()) {
      return originalValue;
    }

    int maskLength = value.length() - keepFirst - keepLast;
    return value.substring(0, keepFirst)
      + "*".repeat(maskLength)
      + value.substring(value.length() - keepLast);
  }
}


class AppendContext {
  JSONArray data;
  int retryCount = 0;
  int records = 0;
  int approximateSize = 0;
  Position position;
  public ArrayList<AbstractAsyncProducer.CallbackCompleter> callbacks;
  public ScheduledFuture<?> scheduledTask;
  final JsonStreamWriter streamWriter;
  final boolean forDdl;

  AppendContext(JsonStreamWriter streamWriter, boolean forDdl) {
    this.streamWriter = streamWriter;
    this.forDdl = forDdl;
    this.data = new JSONArray();
    this.retryCount = 0;
    this.records = 0;
    this.approximateSize = 0;
    this.callbacks = new ArrayList<AbstractAsyncProducer.CallbackCompleter>();
  }

  public void addRow(RowMap r, JSONObject record, AbstractAsyncProducer.CallbackCompleter cc) {
    this.data.put(record);
    this.approximateSize += getJsonByteSize(record);
    this.callbacks.add(cc);
    if(this.position == null) {
        this.position = r.getNextPosition();
    }
  }

  public static int getJsonByteSize(Object json) {
    // Estimate byte size. UTF-8 encoding is assumed, which is standard for JSON.
    // This is an approximation; actual gRPC message size might differ slightly.
    return json.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
  }

  public int getApproximateSize() {
    return approximateSize;
  }

}
