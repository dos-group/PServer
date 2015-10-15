package de.tuberlin.pserver.runtime.partitioning;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.partitioner.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.partitioner.NoPartitioner;
import de.tuberlin.pserver.runtime.partitioning.partitioner.RowPartitioner;
import de.tuberlin.pserver.utils.MatrixBuilder;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIteratorProducer;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class MatrixPartitionManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public final class MatrixLoadTask {

        final ProgramContext        programContext;
        final StateDescriptor       decl;
        final FileDataIterator      fileIterator;
        final IMatrixPartitioner    partitioner;

        public MatrixLoadTask(ProgramContext programContext, StateDescriptor decl) {
            this.programContext  = programContext;
            this.decl         = decl;

            IRecordIteratorProducer recordFormatConfig;
            try {
                recordFormatConfig = decl.recordFormatConfigClass.newInstance();
            }
            catch(IllegalAccessException | InstantiationException e) {
                throw new RuntimeException("Could not instantiate RecordFormatConfig", e);
            }

            this.partitioner = IMatrixPartitioner.newInstance(
                    decl.partitionerClass,
                    decl.rows, decl.cols,
                    programContext.runtimeContext.nodeID,
                    decl.atNodes
            );

            this.fileIterator = fileSystemManager.createFileIterator(
                    decl.path,
                    recordFormatConfig,
                    partitioner
            );
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MatrixPartitionManager.class);

    private final NetManager netManager;

    private final FileSystemManager fileSystemManager;

    private final RuntimeManager runtimeManager;

    private final Map<String, MatrixLoadTask> matrixLoadTasks;

    private final Map<String, AtomicInteger> fileLoadingBarrier;

    private final Map<String, Matrix> loadingMatrices;

    private CountDownLatch finishedLoadingLatch;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixPartitionManager(final NetManager netManager,
                                  final FileSystemManager fileManager,
                                  final RuntimeManager runtimeManager) {

        this.netManager         = Preconditions.checkNotNull(netManager);
        this.fileSystemManager  = fileManager;
        this.runtimeManager     = Preconditions.checkNotNull(runtimeManager);
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
                        synchronized (matrix) {
                            matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                        }
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

    public Matrix addLoadTaskReturnFutureTarget(final ProgramContext programContext, final StateDescriptor stateDescriptor) {
        final MatrixLoadTask mlt = new MatrixLoadTask(programContext, stateDescriptor);
        matrixLoadTasks.put(stateDescriptor.stateName, mlt);
        fileLoadingBarrier.put(stateDescriptor.stateName, new AtomicInteger(programContext.nodeDOP));
        return getLoadingMatrix(mlt);
    }

    public void loadFilesIntoDHT() {
        // Skip, if no loading is required.
        if (matrixLoadTasks.size() == 0) {
            return;
        }
        fileSystemManager.computeInputSplitsForRegisteredFiles();
        finishedLoadingLatch = new CountDownLatch(matrixLoadTasks.size());
        for (final MatrixLoadTask task : matrixLoadTasks.values()) {
            loadMatrix(task);
        }
        while(finishedLoadingLatch.getCount() > 0) {
            LOG.debug("waiting for " + finishedLoadingLatch.getCount() + " loading tasks to finish:");
            for(String taskName : fileLoadingBarrier.keySet()) {
                LOG.debug("task '"+taskName+"' has " + fileLoadingBarrier.get(taskName) + " nodes to finish");
            }
            try {
                finishedLoadingLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
        }
        LOG.info("completed all loading tasks");
    }

    public void clearContext() {
        matrixLoadTasks.clear();
        fileLoadingBarrier.clear();
        loadingMatrices.clear();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Matrix getLoadingMatrix(final MatrixLoadTask task) {
        Matrix matrix = loadingMatrices.get(task.decl.stateName);
        if (matrix == null) {
            matrix = MatrixBuilder.fromMatrixLoadTask(task.decl, task.programContext);
            loadingMatrices.put(task.decl.stateName, matrix);
        }
        return matrix;
    }

    @SuppressWarnings("unchecked")
    private void loadMatrix(final MatrixLoadTask task) {
        int nodeId = task.programContext.runtimeContext.nodeID;

        // prepare to read entries that belong to foreign matrix partitions
        Map<Integer, List<MatrixEntry>> foreignEntries = new HashMap<>();
        // threshold that indicates how many entries are gathered before sending
        int foreignEntriesThreshold = 2048;
        final Matrix matrix = getLoadingMatrix(task);
        final IMatrixPartitioner matrixPartitioner = task.partitioner; //new RowPartitioner(task.decl.rows, task.decl.cols, nodeId, task.decl.atNodes);
        final FileDataIterator<? extends IRecord> fileIterator = task.fileIterator;
        ReusableMatrixEntry reusable = new MutableMatrixEntry(-1, -1, Double.NaN);

        MatrixEntry entry;
        while (fileIterator.hasNext()) {
            final IRecord record = fileIterator.next();

            // iterate through entries in record
            while (record.hasNext()) {
                entry = record.next(reusable);

                if(entry.getRow() >= task.decl.rows || entry.getCol() >= task.decl.cols)
                    continue;

                // get the partition this record belongs to
                int targetPartition = matrixPartitioner.getPartitionOfEntry(entry);

                // if record belongs to own node, set the value
                if (targetPartition == nodeId || targetPartition == -1) {
                    synchronized (matrix) {
                        matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                    }
                } else {
                    // otherwise append entry to foreign entries and send them depending on threshold
                    final List<MatrixEntry> foreignsOfTargetNode = getListOrCreateIfNotExists(foreignEntries, targetPartition, foreignEntriesThreshold);
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
        netManager.broadcastEvent(new MatrixPartitionManager.FinishedLoadingFileEvent(task.decl.stateName));
        finishedTask(task.decl.stateName);
    }

    /**
     * Is called whenever an at finished processing an input split. This is triggered either by reaching the end
     * of the own input split or by receiving a @link FinishedLoadingFileEvent. If all at finished processing,
     * the matrix can be put into the DHT.
     */
    private void finishedTask(final String name) {
        final int counter = fileLoadingBarrier.get(name).decrementAndGet();
        if (counter <= 0) {
            // is it possible that a FinishedLoading event overtakes a SendPartition event?
            // this assumes it is not:
            final Matrix matrix = loadingMatrices.get(name);
            runtimeManager.putDHT(name, matrix);
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
            netManager.sendEvent(targetNodeId, new MatrixPartitionManager.MatrixEntryPartitionEvent(entriesArray, task.decl.stateName));
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
