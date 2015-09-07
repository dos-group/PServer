package de.tuberlin.pserver.runtime.partitioning;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.dsl.state.GlobalScope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.config.AbstractRecordFormatConfig;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class MatrixPartitionManager {

    private Logger LOG = Logger.getLogger(MatrixPartitionManager.class);

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class MatrixLoadTask {

        final SlotContext slotContext;
        final String name;
        final long rows;
        final long cols;
        final GlobalScope globalScope;
        final PartitionType partitionType;
        final AbstractRecordFormatConfig recordFormat;
        final Format matrixFormat;
        final Layout matrixLayout;
        final FileDataIterator fileIterator;

        public MatrixLoadTask(final SlotContext slotContext,
                              final String filePath,
                              final String name,
                              final long rows,
                              final long cols,
                              final GlobalScope globalScope,
                              final PartitionType partitionType,
                              final AbstractRecordFormatConfig recordFormat,
                              final Format matrixFormat,
                              final Layout matrixLayout) {

            this.slotContext        = slotContext;
            this.name               = name;
            this.rows               = rows;
            this.cols               = cols;
            this.globalScope        = globalScope;
            this.partitionType      = partitionType;
            this.recordFormat       = recordFormat;
            this.matrixFormat       = matrixFormat;
            this.matrixLayout       = matrixLayout;
            this.fileIterator       = fileSystemManager.createFileIterator(filePath, recordFormat, partitionType);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final NetManager netManager;

    private final FileSystemManager fileSystemManager;

    private final DataManager dataManager;

    private final Map<String, MatrixLoadTask> matrixLoadTasks;

    private final Map<String, AtomicInteger> fileLoadingBarrier;

    private final Map<String, Matrix> loadingMatrices;

    private CountDownLatch finishedLoadingLatch;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixPartitionManager(final NetManager netManager,
                                  final FileSystemManager fileSystemManager,
                                  final DataManager dataManager) {

        this.netManager         = Preconditions.checkNotNull(netManager);
        this.fileSystemManager  = fileSystemManager;
        this.dataManager        = Preconditions.checkNotNull(dataManager);
        this.matrixLoadTasks    = new ConcurrentHashMap<>();
        this.fileLoadingBarrier = new ConcurrentHashMap<>();
        this.loadingMatrices    = new ConcurrentHashMap<>();

        this.netManager.addEventListener(
                MatrixEntryPartitionEvent.MATRIX_ENTRY_PARTITION_EVENT,
                (event) -> {
                    final MatrixEntryPartitionEvent e = (MatrixEntryPartitionEvent) event;
                    final MatrixLoadTask task = matrixLoadTasks.get(e.getName());
                    final Matrix matrix = getLoadingMatrix(task);
                    for (final MatrixEntry entry : e.getEntries()) {
                        if(entry.getRow() == 5990 && entry.getCol() == 529) {
                            LOG.error("got the fucker via event: " + entry);
                        }
                        matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                    }
                }
        );

        this.netManager.addEventListener(
                FinishedLoadingFileEvent.FINISHED_LOADING_FILE_EVENT,
                (event) -> finishedTask(((FinishedLoadingFileEvent)event).getName())
        );
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void load(final SlotContext slotContext,
                     final String filePath,
                     final String name,
                     final long rows, final long cols,
                     final GlobalScope globalScope,
                     final PartitionType partitionType,
                     final AbstractRecordFormatConfig recordFormat,
                     final Format matrixFormat,
                     final Layout matrixLayout) {

        final MatrixLoadTask mlt = new MatrixLoadTask(
                slotContext,
                filePath,
                name,
                rows, cols,
                globalScope,
                globalScope == GlobalScope.REPLICATED ? PartitionType.NOT_PARTITIONED : partitionType,
                recordFormat,
                matrixFormat,
                matrixLayout
        );
        matrixLoadTasks.put(name, mlt);
        fileLoadingBarrier.put(name, new AtomicInteger(slotContext.programContext.nodeDOP));
    }

    public void loadFilesIntoDHT() {
        fileSystemManager.computeInputSplitsForRegisteredFiles();
        finishedLoadingLatch = new CountDownLatch(matrixLoadTasks.size());
        for (final MatrixLoadTask task : matrixLoadTasks.values()) {
            switch (task.partitionType) {
                case NOT_PARTITIONED: loadMatrix_notPartitioned(task); break;
                case ROW_PARTITIONED: loadMatrix_rowPartitioned(task); break;
                case COLUMN_PARTITIONED: throw new UnsupportedOperationException();
                case BLOCK_PARTITIONED: throw new UnsupportedOperationException();
                default: throw new IllegalStateException();
            }
        }
        while(finishedLoadingLatch.getCount() > 0) {
            try {
                finishedLoadingLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.debug("waiting for " + finishedLoadingLatch.getCount() + " loading tasks to finish");
            }
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Matrix getLoadingMatrix(final MatrixLoadTask task) {
        Matrix matrix = loadingMatrices.get(task.name);
        if (matrix == null) {
            switch (task.globalScope) {
                case REPLICATED: {
                    matrix = new MatrixBuilder()
                            .dimension(task.rows, task.cols)
                            .format(task.matrixFormat)
                            .layout(task.matrixLayout)
                            .build();
                } break;
                case PARTITIONED: {
                    matrix = new DistributedMatrix(
                            task.slotContext,
                            task.rows,
                            task.cols,
                            task.partitionType,
                            task.matrixLayout,
                            task.matrixFormat,
                            false,
                            //(int)task.fileIterator.getFileSection().linesToRead
                            0
                    );
                } break;
                case LOGICALLY_PARTITIONED:
                    matrix = new DistributedMatrix(
                            task.slotContext,
                            task.rows,
                            task.cols,
                            task.partitionType,
                            task.matrixLayout,
                            task.matrixFormat,
                            true
                    );
                    break;
            }
            loadingMatrices.put(task.name, matrix);
        }
        return matrix;
    }

    @SuppressWarnings("unchecked")
    private void loadMatrix_notPartitioned(final MatrixLoadTask task) {
        final FileDataIterator<? extends IRecord> fileIterator = task.fileIterator;
        final Matrix matrix = getLoadingMatrix(task);
        ReusableMatrixEntry reusable = new MutableMatrixEntry(-1, -1, Double.NaN);
        while (fileIterator.hasNext()) {
            final IRecord record = fileIterator.next();
            // iterate through entries in record
            while (record.hasNext()) {
                MatrixEntry entry = record.next(reusable);
                if(entry.getRow() >= task.rows || entry.getCol() >= task.cols)
                    continue;
                matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
            }
        }
        netManager.broadcastEvent(new MatrixPartitionManager.FinishedLoadingFileEvent(task.name));
        finishedTask(task.name);
    }

    @SuppressWarnings("unchecked")
    private void loadMatrix_rowPartitioned(final MatrixLoadTask task) {
        // prepare to read entries that belong to foreign matrix partitions
        Map<Integer, List<MatrixEntry>> foreignEntries = new HashMap<>();
        // threshold that indicates how many entries are gathered before sending
        int foreignEntriesThreshold = 2048;
        final IMatrixPartitioner matrixPartitioner = new MatrixByRowPartitioner(
                task.slotContext.programContext.runtimeContext.nodeID,
                task.slotContext.programContext.nodeDOP,
                task.rows,
                task.cols
        );
        final FileDataIterator<? extends IRecord> fileIterator = task.fileIterator;
        final Matrix matrix = getLoadingMatrix(task);
        ReusableMatrixEntry reusable = new MutableMatrixEntry(-1, -1, Double.NaN);

        int nodeId = task.slotContext.programContext.runtimeContext.nodeID;
        MatrixEntry entry = null;
        while (fileIterator.hasNext()) {
            final IRecord record = fileIterator.next();
            // iterate through entries in record
            while (record.hasNext()) {
                entry = record.next(reusable);
                if(entry.getRow() == 5990 && entry.getCol() == 529) {
                    LOG.error("read the fucker from disk: " + entry);
                    if(entry.getRow() >= task.rows || entry.getCol() >= task.cols)
                        LOG.error("discarded the fucker!!!" + entry.getRow() + " >= "+ task.rows + " || " + entry.getCol() + " >= " + task.cols);
                }
                if(entry.getRow() >= task.rows || entry.getCol() >= task.cols)
                    continue;
                // get the partition this record belongs to
                int targetPartition = matrixPartitioner.getPartitionOfEntry(entry);
                // if record belongs to own node, set the value
                if(entry.getRow() == 5990 && entry.getCol() == 529)
                    LOG.error("fuckers partition: "+targetPartition+" my partition: "+nodeId);
                if (targetPartition == nodeId) {
                    matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                } else {
                    if(entry.getRow() == 5990 && entry.getCol() == 529)
                        LOG.error("read the fucker from disk: " + entry);
                    // otherwise append entry to foreign entries and send them depending on threshold
                    List<MatrixEntry> foreignsOfTargetNode = getListOrCreateIfNotExists(foreignEntries, targetPartition, foreignEntriesThreshold);
                    foreignsOfTargetNode.add(new ImmutableMatrixEntry(entry));
                    if (foreignsOfTargetNode.size() >= foreignEntriesThreshold) {
                        sendPartition(targetPartition, foreignsOfTargetNode, task);
                    }
                }
            }
        }
        // send all remaining foreign entries
        for (Map.Entry<Integer, List<MatrixEntry>> map : foreignEntries.entrySet()) {
            sendPartition(map.getKey(), map.getValue(), task);
        }
        netManager.broadcastEvent(new MatrixPartitionManager.FinishedLoadingFileEvent(task.name));
        finishedTask(task.name);
    }

    /**
     * Is called whenever an nodes finished processing an input split. This is triggered either by reaching the end
     * of the own input split or by receiving a @link FinishedLoadingFileEvent. If all nodes finished processing,
     * the matrix can be put into the DHT.
     */
    private void finishedTask(final String name) {
        final int counter = fileLoadingBarrier.get(name).decrementAndGet();
        if (counter <= 0) {
            // is it possible that a FinishedLoading event overtakes a SendPartition event?
            // this assumes it is not:
            final Matrix matrix = loadingMatrices.get(name);
            dataManager.putObject(name, matrix);
            finishedLoadingLatch.countDown();
        }
    }

    private List<MatrixEntry> getListOrCreateIfNotExists(Map<Integer, List<MatrixEntry>> foreignEntries, int partitionId, int threshold) {
        List<MatrixEntry> result = foreignEntries.get(partitionId);
        // if list does not exist yet, create and put it into map
        if (result == null) {
            result = new ArrayList<>(threshold);
            foreignEntries.put(partitionId, result);
        }
        return result;
    }

    private void sendPartition(int targetNodeId, List<MatrixEntry> entries, MatrixLoadTask task) {
        if (entries != null && !entries.isEmpty()) {
            MatrixEntry[] entriesArray = entries.toArray(new MatrixEntry[entries.size()]);
            netManager.sendEvent(targetNodeId, new MatrixPartitionManager.MatrixEntryPartitionEvent(entriesArray, task.name));
            entries.clear();
        }
    }

    // ---------------------------------------------------
    // Events.
    // ---------------------------------------------------

    /**
     * Is send from an node, that loads "foreign" matrix entries from its input files.<br>
     * From the perspective of one node, foreign matrix entries are those belonging to another node.<br>
     * Received from an node, to that the containing entries belong to.
     */
    public static final class MatrixEntryPartitionEvent extends NetEvents.NetEvent {

        public static final String MATRIX_ENTRY_PARTITION_EVENT = "MATRIX_ENTRY_PARTITION_EVENT";
        private static final long serialVersionUID = -1L;
        private final MatrixEntry[] entries;
        private final String name;

        public MatrixEntryPartitionEvent(MatrixEntry[] entries, String name) {
            super(MATRIX_ENTRY_PARTITION_EVENT);
            this.entries = Preconditions.checkNotNull(entries);
            this.name = Preconditions.checkNotNull(name);
        }

        public MatrixEntry[] getEntries() { return entries; }
        public String getName() { return name; }
    }

    // ---------------------------------------------------

    public static final class FinishedLoadingFileEvent extends NetEvents.NetEvent {

        public static final String FINISHED_LOADING_FILE_EVENT = "FINISHED_LOADING_FILE_EVENT";
        private static final long serialVersionUID = -1L;
        private final String name;

        public FinishedLoadingFileEvent(String name) {
            super(FINISHED_LOADING_FILE_EVENT);
            this.name = Preconditions.checkNotNull(name);
        }

        public String getName() { return name; }
    }
}
