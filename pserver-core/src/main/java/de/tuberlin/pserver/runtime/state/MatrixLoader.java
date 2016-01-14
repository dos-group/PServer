package de.tuberlin.pserver.runtime.state;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.state.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.state.mtxentries.ReusableMatrixEntry;
import de.tuberlin.pserver.runtime.state.partitioner.IMatrixPartitioner;
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

public final class MatrixLoader {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public final class MatrixLoadTask<MAT extends MatrixBase> {

        final ProgramContext programContext;

        final MAT matrix;

        final StateDescriptor state;

        final IMatrixPartitioner partitioner;

        final FileDataIterator fileIterator;

        public MatrixLoadTask(final ProgramContext programContext,
                              final StateDescriptor state,
                              final MAT matrix) {

            this.programContext = Preconditions.checkNotNull(programContext);
            this.state = Preconditions.checkNotNull(state);
            this.matrix = Preconditions.checkNotNull(matrix);

            this.partitioner = IMatrixPartitioner.newInstance(
                    state.partitioner,
                    state.rows,
                    state.cols,
                    programContext.nodeID,
                    state.atNodes
            );

            try {
                this.fileIterator = fileManager.createFileIterator(
                        state.path,
                        state.recordFormat.newInstance(),
                        partitioner
                );
            } catch(IllegalAccessException | InstantiationException e) {
                throw new RuntimeException("Could not instantiate RecordFormatConfig", e);
            }
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MatrixLoader.class);

    private final NetManager netManager;

    private final FileSystemManager fileManager;

    private final Map<String, MatrixLoadTask<?>> matrixLoadTasks;

    private final Map<String, AtomicInteger> fileLoadingBarrier;

    private final Map<String, MatrixBase> loadingMatrices;

    private CountDownLatch finishedLoadingLatch;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixLoader(final NetManager netManager, final FileSystemManager fileManager) {

        this.netManager         = Preconditions.checkNotNull(netManager);
        this.fileManager        = Preconditions.checkNotNull(fileManager);
        this.matrixLoadTasks    = new ConcurrentHashMap<>();
        this.fileLoadingBarrier = new ConcurrentHashMap<>();
        this.loadingMatrices    = new ConcurrentHashMap<>();

        final AtomicInteger partitionEventCounter = new AtomicInteger(0);

        this.netManager.addEventListener(
                MatrixEntryPartitionEvent.MATRIX_ENTRY_PARTITION_EVENT,
                (event) -> {
                    final MatrixEntryPartitionEvent e = (MatrixEntryPartitionEvent) event;
                    final MatrixLoadTask task = matrixLoadTasks.get(e.getName());
                    final Matrix matrix = (Matrix)loadingMatrices.get(task.state.stateName);
                    synchronized (matrix) {
                        if (Matrix32F.class.isAssignableFrom(matrix.getClass())) {
                            synchronized (matrix) {
                                for (final MatrixEntry entry : e.getEntries()) {
                                    matrix.set(entry.getRow(), entry.getCol(), entry.getValue().floatValue());
                                }
                            }
                        }

                        if (Matrix64F.class.isAssignableFrom(matrix.getClass())) {
                            synchronized (matrix) {
                                for (final MatrixEntry entry : e.getEntries()) {
                                    matrix.set(entry.getRow(), entry.getCol(), entry.getValue().doubleValue());
                                }
                            }
                        }
                    }

                    if (partitionEventCounter.get() % 50000 == 0) {
                        LOG.info("Received " + partitionEventCounter.get() + " from " + e.srcMachineID.toString());
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

    public <MAT extends MatrixBase> void addLoadingTask(final ProgramContext programContext,
                                                        final StateDescriptor stateDescriptor,
                                                        final MAT matrix) {

        final MatrixLoadTask<MAT> mlt = new MatrixLoadTask<MAT>(programContext, stateDescriptor, matrix);
        loadingMatrices.put(stateDescriptor.stateName, matrix);
        matrixLoadTasks.put(stateDescriptor.stateName, mlt);
        fileLoadingBarrier.put(stateDescriptor.stateName, new AtomicInteger(programContext.nodeDOP));
    }

    public void loadFilesIntoDHT() {
        // Skip, if no loading is required.
        if (matrixLoadTasks.size() == 0)
            return;

        fileManager.computeInputSplitsForRegisteredFiles();
        finishedLoadingLatch = new CountDownLatch(matrixLoadTasks.size());
        matrixLoadTasks.values().forEach(this::loadMatrix);

        while(finishedLoadingLatch.getCount() > 0) {
            LOG.debug("waiting for " + finishedLoadingLatch.getCount() + " loading tasks to finish:");
            for(String taskName : fileLoadingBarrier.keySet()) {
                LOG.debug("task '"+taskName+"' has " + fileLoadingBarrier.get(taskName) + " stateObjectNodes to finish");
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

    @SuppressWarnings("unchecked")
    private void loadMatrix(final MatrixLoadTask task) {
        int nodeId = task.programContext.nodeID;
        // prepare to read entries that belong to foreign matrix partitions
        Map<Integer, List<MatrixEntry>> foreignEntries = new HashMap<>();
        // threshold that indicates how many entries are gathered before sending
        int foreignEntriesThreshold = 4096 * 2;
        final Matrix matrix = (Matrix)loadingMatrices.get(task.state.stateName);
        final IMatrixPartitioner matrixPartitioner = task.partitioner;
        final FileDataIterator<? extends IRecord> fileIterator = task.fileIterator;
        ReusableMatrixEntry reusable = new MutableMatrixEntry(-1, -1, Double.NaN);

        MatrixEntry entry;
        while (fileIterator.hasNext()) {

            final IRecord record = fileIterator.next();

            synchronized (matrix) {
                // iterate through entries in record
                while (record.hasNext()) {
                    entry = record.next(reusable);
                    if (entry.getRow() >= task.state.rows || entry.getCol() >= task.state.cols)
                        continue;
                    // get the partition this record belongs to
                    int targetPartition = matrixPartitioner.getPartitionOfEntry(entry);
                    // if record belongs to own node, set the value
                    if (targetPartition == nodeId) {

                        if (Matrix32F.class.isAssignableFrom(matrix.getClass())) {
                            matrix.set(entry.getRow(), entry.getCol(), entry.getValue().floatValue());
                        }
                        if (Matrix64F.class.isAssignableFrom(matrix.getClass())) {
                            matrix.set(entry.getRow(), entry.getCol(), entry.getValue().doubleValue());
                        }
                    } else {
                        // otherwise append entry to foreign entries and send them depending on threshold
                        List<MatrixEntry> valuesOfTargetNode = getListOrCreateIfNotExists(foreignEntries, targetPartition, foreignEntriesThreshold);
                        valuesOfTargetNode.add(new ImmutableMatrixEntry(entry));
                        if (valuesOfTargetNode.size() >= foreignEntriesThreshold) {
                            sendPartition(targetPartition, valuesOfTargetNode, task);
                        }
                    }
                }
            }
        }
        // send all remaining foreign entries
        //for (Map.Entry<Integer, List<MatrixEntry>> map : foreignEntries.entrySet()) {
        //    sendPartition(map.getKey(), map.getValue(), task);
        //}
        netManager.broadcastEvent(new MatrixLoader.FinishedLoadingFileEvent(task.state.stateName));
        finishedTask(task.state.stateName);
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
            // this assumes it is not:;
            finishedLoadingLatch.countDown();
        }
    }

    private List<MatrixEntry> getListOrCreateIfNotExists(Map<Integer, List<MatrixEntry>> entries, int partitionId, int threshold) {
        List<MatrixEntry> result = entries.get(partitionId);
        // if list does not exist yet, create and put it into map
        if (result == null) {
            result = new ArrayList<>(threshold);
            entries.put(partitionId, result);
        }
        return result;
    }

    private void sendPartition(int targetNodeId, List<MatrixEntry> entries, MatrixLoadTask task) {
        if (entries != null && !entries.isEmpty()) {
            MatrixEntry[] entriesArray = entries.toArray(new MatrixEntry[entries.size()]);
            netManager.dispatchEventAt(new int[] {targetNodeId}, new MatrixLoader.MatrixEntryPartitionEvent(entriesArray, task.state.stateName));
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
    public static final class MatrixEntryPartitionEvent extends NetEvent {

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

    public static final class FinishedLoadingFileEvent extends NetEvent {

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