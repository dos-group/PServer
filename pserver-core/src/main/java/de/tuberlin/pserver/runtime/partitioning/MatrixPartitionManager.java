package de.tuberlin.pserver.runtime.partitioning;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.MLProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.RecordFormat;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ImmutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.ReusableMatrixEntry;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public final class MatrixPartitionManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class MatrixLoadTask {


        final String name;
        final FileDataIterator fileIterator;
        final RecordFormat recordFormat;
        final long rows;
        final long cols;
        final Format matrixFormat;
        final Layout matrixLayout;
        final IMatrixPartitioner matrixPartitioner;

        public MatrixLoadTask(String filePath, String name, RecordFormat recordFormat, long rows, long cols,
                              Format matrixFormat, Layout matrixLayout,
                              IMatrixPartitioner matrixPartitioner) {

            this.fileIterator = fileSystemManager.createFileIterator(filePath, recordFormat);
            this.name = name;
            this.recordFormat = recordFormat;
            this.rows = rows;
            this.cols = cols;
            this.matrixFormat = matrixFormat;
            this.matrixLayout = matrixLayout;
            this.matrixPartitioner = matrixPartitioner;
        }
    }

    // ---------------------------------------------------
    // Events.
    // ---------------------------------------------------

    public static final class Events {

        public static final String FINISHED_LOADING_FILE_EVENT = "FINISHED_LOADING_FILE_EVENT";

        public static final String MATRIX_ENTRY_PARTITION_EVENT = "MATRIX_ENTRY_PARTITION_EVENT";

    }

    /**
     * Is send from an node, that loads "foreign" matrix entries from its input files.<br>
     * From the perspective of one node, foreign matrix entries are those belonging to another node.<br>
     * Received from an node, to that the containing entries belong to.
     */
    public static final class MatrixEntryPartitionEvent extends NetEvents.NetEvent {

        private static final long serialVersionUID = -1L;

        private final MatrixEntry[] entries;

        private final String name;

        public MatrixEntryPartitionEvent(MatrixEntry[] entries, String name) {
            super(Events.MATRIX_ENTRY_PARTITION_EVENT);
            this.entries = Preconditions.checkNotNull(entries);
            this.name = Preconditions.checkNotNull(name);
        }

        public MatrixEntry[] getEntries() {
            return entries;
        }

        public String getName() {
            return name;
        }
    }

    public static final class FinishedLoadingFileEvent extends NetEvents.NetEvent {

        private static final long serialVersionUID = -1L;

        private final String name;

        public FinishedLoadingFileEvent(String name) {
            super(Events.FINISHED_LOADING_FILE_EVENT);
            this.name = Preconditions.checkNotNull(name);
        }

        public String getName() {
            return name;
        }
    }

    public final class MatrixEntryPartitionEventHandler implements IEventHandler {

        @Override
        public void handleEvent(Event event) {
            MatrixPartitionManager.MatrixEntryPartitionEvent e = Preconditions.checkNotNull((MatrixPartitionManager.MatrixEntryPartitionEvent) event);
            MatrixLoadTask task = matrixLoadTasks.get(e.getName());
            Matrix.PartitionShape partitionShape = task.matrixPartitioner.getPartitionShape();
            Matrix matrix = getLoadingMatrix(task);
            synchronized (matrix) {
                for (MatrixEntry entry : e.getEntries()) {
                    // HOTFIX: partition aware matrix
                    matrix.set(entry.getRow() % partitionShape.getRows(), entry.getCol() % partitionShape.getCols(), entry.getValue());
                    //matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                }
            }
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MatrixPartitionManager.class);

    private final IConfig config;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final FileSystemManager fileSystemManager;

    private final DataManager dataManager;

    private final Map<String, MatrixLoadTask> matrixLoadTasks;

    private final Map<String, AtomicInteger> fileLoadingBarrier;

    private CountDownLatch finishedLoadingLatch;

    private final Map<String, Matrix> loadingMatrices;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixPartitionManager(final IConfig config,
                                  final InfrastructureManager infraManager,
                                  final NetManager netManager,
                                  final FileSystemManager fileSystemManager,
                                  final DataManager dataManager) {

        this.config             = Preconditions.checkNotNull(config);
        this.infraManager       = Preconditions.checkNotNull(infraManager);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.fileSystemManager  = fileSystemManager;
        this.dataManager        = Preconditions.checkNotNull(dataManager);

        this.matrixLoadTasks    = new HashMap<>();
        this.fileLoadingBarrier = new HashMap<>();
        this.loadingMatrices    = new HashMap<>();

        this.netManager.addEventListener(MatrixPartitionManager.Events.MATRIX_ENTRY_PARTITION_EVENT, new MatrixEntryPartitionEventHandler());
        this.netManager.addEventListener(MatrixPartitionManager.Events.FINISHED_LOADING_FILE_EVENT, event -> {
            MatrixPartitionManager.FinishedLoadingFileEvent e = Preconditions.checkNotNull((FinishedLoadingFileEvent) event);
            nodeFinishedProcessingSplit(e.getName());
        });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void load(final String filePath,
                     final String name,
                     final long rows, final long cols,
                     final RecordFormat recordFormat,
                     final Format matrixFormat,
                     final Layout matrixLayout,
                     final IMatrixPartitioner matrixPartitioner,
                     final MLProgramContext programContext) {

        matrixLoadTasks.put(filePath, new MatrixLoadTask(filePath, name, recordFormat, rows, cols,
                matrixFormat, matrixLayout, matrixPartitioner));
        fileLoadingBarrier.put(filePath, new AtomicInteger(programContext.nodeDOP));
    }

    public void loadFilesIntoDHT() throws Exception {
        finishedLoadingLatch = new CountDownLatch(matrixLoadTasks.size());
        // prepare to read entries that belong to foreign matrix partitions
        Map<Integer, List<MatrixEntry>> foreignEntries = new HashMap<Integer, List<MatrixEntry>>(); // data structure to hold foreign entries
        int foreignEntriesThreshold = 2048; // threshold that indicates how many entries are gathered before sending
        // iterate through load tasks
        for (final MatrixLoadTask task : matrixLoadTasks.values()) {
            //final MatrixLoadTask task = en.getValue();
            // preallocate local matrix partition
            Matrix matrix = getLoadingMatrix(task);
            // iterate through records in file
            @SuppressWarnings("unchecked")
            FileDataIterator<? extends IRecord> fileIterator = task.fileIterator;
            Matrix.PartitionShape partitionShape = task.matrixPartitioner.getPartitionShape();
            ReusableMatrixEntry reusable = new MutableMatrixEntry(-1, -1, Double.NaN);
            while (fileIterator.hasNext()) {
                final IRecord record = fileIterator.next();
                // iterate through entries in record
                synchronized (matrix) {
                    while (record.hasNext()) {
                        MatrixEntry entry = record.next(reusable);
                        if(entry.getRow() >= task.rows || entry.getCol() >= task.cols) {
                            continue;
                        }
                        //System.out.println(nodeID + ": " + entry);
                        // get the partition this record belongs to
                        int targetPartition = task.matrixPartitioner.getPartitionOfEntry(entry);
                        // if record belongs to own node, set the value
                        if (targetPartition == infraManager.getNodeID()) {
                            // set entry
                            // HOTFIX: partition aware matrix
                            matrix.set(entry.getRow() % partitionShape.getRows(), entry.getCol() % partitionShape.getCols(), entry.getValue());
                            //matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                        }
                        // otherwise append entry to foreign entries and send them depending on threshold
                        else {
                            List<MatrixEntry> foreignsOfTargetNode = getSavely(foreignEntries, targetPartition, foreignEntriesThreshold);
                            foreignsOfTargetNode.add(new ImmutableMatrixEntry(entry));
                            if (foreignsOfTargetNode.size() >= foreignEntriesThreshold) {
                                // send them
                                sendPartition(targetPartition, foreignsOfTargetNode, task);
                            }
                        } // </partition check>
                    } // </entries in record iteration
                }
            } // </records in file iteration>>

            // send all remaining foreign entries
            for (Map.Entry<Integer, List<MatrixEntry>> map : foreignEntries.entrySet()) {
                sendPartition(map.getKey(), map.getValue(), task);
            }

            netManager.broadcastEvent(new MatrixPartitionManager.FinishedLoadingFileEvent(fileIterator.getFilePath()));

            nodeFinishedProcessingSplit(fileIterator.getFilePath());
        }
        finishedLoadingLatch.await();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Matrix getLoadingMatrix(final MatrixLoadTask task) {
        synchronized (loadingMatrices) {
            String name = task.fileIterator.getFilePath();
            Matrix matrix = loadingMatrices.get(name);
            if (matrix == null) {
                Matrix.PartitionShape partitionShape = task.matrixPartitioner.getPartitionShape();
                matrix = new MatrixBuilder()
                        .dimension(partitionShape.getRows(), partitionShape.getCols())
                                //.dimension(task.rows, task.cols)
                        .format(task.matrixFormat)
                        .layout(task.matrixLayout)
                        .build();

                loadingMatrices.put(name, matrix);
            }
            return matrix;
        }
    }

    /**
     * Is called whenever an nodes finished processing an input split. This is triggered either by reaching the end
     * of the own input split or by receiving a @link FinishedLoadingFileEvent. If all nodes finished processing,
     * the matrix can be put into the DHT.
     *
     * @param name
     */
    private void nodeFinishedProcessingSplit(String name) {
        int counter;
        synchronized (fileLoadingBarrier) {
            counter = fileLoadingBarrier.get(name).decrementAndGet();
        }
        if (counter <= 0) {
            // is it possible that a FinishedLoading event overtakes a SendPartition event?
            // this assumes it is not:
            Matrix matrix;
            synchronized (loadingMatrices) {
                matrix = Preconditions.checkNotNull(loadingMatrices.get(name));
            }
            synchronized (matrix) {
                dataManager.putObject(name, matrix);
                finishedLoadingLatch.countDown();
            }
        }
    }

    private List<MatrixEntry> getSavely(Map<Integer, List<MatrixEntry>> foreignEntries, int partitionId, int threshold) {
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
            netManager.sendEvent(targetNodeId, new MatrixPartitionManager.MatrixEntryPartitionEvent(entriesArray, task.fileIterator.getFilePath()));
            entries.clear();
        }
    }
}
