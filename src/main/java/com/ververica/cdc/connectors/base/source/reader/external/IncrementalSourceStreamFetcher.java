//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.ververica.cdc.connectors.base.source.reader.external;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class IncrementalSourceStreamFetcher implements Fetcher<SourceRecords, SourceSplitBase> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceStreamFetcher.class);
    private final FetchTask.Context taskContext;
    private final ExecutorService executorService;
    private final Set<TableId> pureStreamPhaseTables;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;
    private FetchTask<SourceSplitBase> streamFetchTask;
    private StreamSplit currentStreamSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    private Map<TableId, Offset> maxSplitHighWatermarkMap;
    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceStreamFetcher(FetchTask.Context taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory = (new ThreadFactoryBuilder()).setNameFormat("debezium-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureStreamPhaseTables = new HashSet();
    }

    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.streamFetchTask = fetchTask;
        this.currentStreamSplit = ((SourceSplitBase)fetchTask.getSplit()).asStreamSplit();
        this.configureFilter();
        this.taskContext.configure(this.currentStreamSplit);
        this.queue = this.taskContext.getQueue();
        this.executorService.submit(() -> {
            try {
                this.streamFetchTask.execute(this.taskContext);
            } catch (Exception var2) {
                Exception e = var2;
                this.currentTaskRunning = false;
                LOG.error(String.format("Execute stream read task for stream split %s fail", this.currentStreamSplit), e);
                this.readException = e;
            }

        });
    }

    public boolean isFinished() {
        return this.currentStreamSplit == null || !this.currentTaskRunning;
    }

    @Nullable
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        this.checkReadException();
        List<SourceRecord> sourceRecords = new ArrayList();
        if (this.currentTaskRunning) {
            List<DataChangeEvent> batch = this.queue.poll();
            Iterator var3 = batch.iterator();

            while(var3.hasNext()) {
                DataChangeEvent event = (DataChangeEvent)var3.next();
                if (this.shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                } else {
                    LOG.debug("{} data change event should not emit", event);
                }
            }

            List<SourceRecords> sourceRecordsSet = new ArrayList();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    private void checkReadException() {
        if (this.readException != null) {
            throw new FlinkRuntimeException(String.format("Read split %s error due to %s.", this.currentStreamSplit, this.readException.getMessage()), this.readException);
        }
    }

    public void close() {
        try {
            this.stopReadTask();
            if (this.executorService != null) {
                this.executorService.shutdown();
                if (!this.executorService.awaitTermination(30L, TimeUnit.SECONDS)) {
                    LOG.warn("Failed to close the stream fetcher in {} seconds.", 30L);
                }
            }
        } catch (Exception var2) {
            Exception e = var2;
            LOG.error("Close stream fetcher error", e);
        }

    }

    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (this.taskContext.isDataChangeRecord(sourceRecord)) {
            TableId tableId = this.taskContext.getTableId(sourceRecord);
            Offset position = this.taskContext.getStreamOffset(sourceRecord);
            if (this.hasEnterPureStreamPhase(tableId, position)) {
                return true;
            } else {
                if (this.finishedSplitsInfo.containsKey(tableId)) {
                    Iterator var4 = ((List)this.finishedSplitsInfo.get(tableId)).iterator();

                    while(var4.hasNext()) {
                        FinishedSnapshotSplitInfo splitInfo = (FinishedSnapshotSplitInfo)var4.next();
                        if (this.taskContext.isRecordBetween(sourceRecord, splitInfo.getSplitStart(), splitInfo.getSplitEnd()) && position.isAfter(splitInfo.getHighWatermark())) {
                            return true;
                        }
                    }
                }

                return false;
            }
        } else {
            return true;
        }
    }

    private boolean hasEnterPureStreamPhase(TableId tableId, Offset position) {
        if (this.pureStreamPhaseTables.contains(tableId)) {
            return true;
        } else if (this.maxSplitHighWatermarkMap.containsKey(tableId) && position.isAtOrAfter((Offset)this.maxSplitHighWatermarkMap.get(tableId))) {
            this.pureStreamPhaseTables.add(tableId);
            return true;
        } else {
            return !this.maxSplitHighWatermarkMap.containsKey(tableId) && this.taskContext.getTableFilter().isIncluded(tableId);
        }
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos = this.currentStreamSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap();
        Map<TableId, Offset> tableIdOffsetPositionMap = new HashMap();
        Iterator var4;
        if (finishedSplitInfos.isEmpty()) {
            var4 = this.currentStreamSplit.getTableSchemas().keySet().iterator();

            while(var4.hasNext()) {
                TableId tableId = (TableId)var4.next();
                tableIdOffsetPositionMap.put(tableId, this.currentStreamSplit.getStartingOffset());
            }
        } else {
            var4 = finishedSplitInfos.iterator();

            label25:
            while(true) {
                TableId tableId;
                Offset highWatermark;
                Offset maxHighWatermark;
                do {
                    if (!var4.hasNext()) {
                        break label25;
                    }

                    FinishedSnapshotSplitInfo finishedSplitInfo = (FinishedSnapshotSplitInfo)var4.next();
                    tableId = finishedSplitInfo.getTableId();
                    List<FinishedSnapshotSplitInfo> list = (List)splitsInfoMap.getOrDefault(tableId, new ArrayList());
                    list.add(finishedSplitInfo);
                    splitsInfoMap.put(tableId, list);
                    highWatermark = finishedSplitInfo.getHighWatermark();
                    maxHighWatermark = (Offset)tableIdOffsetPositionMap.get(tableId);
                } while(maxHighWatermark != null && !highWatermark.isAfter(maxHighWatermark));

                tableIdOffsetPositionMap.put(tableId, highWatermark);
            }
        }

        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdOffsetPositionMap;
        this.pureStreamPhaseTables.clear();
    }

    public void stopReadTask() throws Exception {
        this.currentTaskRunning = false;
        if (this.taskContext != null) {
            this.taskContext.close();
        }

        if (this.streamFetchTask != null) {
            this.streamFetchTask.close();
        }

    }
}
