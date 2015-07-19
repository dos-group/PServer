package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.FileSystemManager;
import de.tuberlin.pserver.app.filesystem.record.IRecord;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.app.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.app.types.ImmutableMatrixEntry;
import de.tuberlin.pserver.app.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.app.types.MObjectValue;
import de.tuberlin.pserver.app.types.*;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.MObject;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataManager extends EventDispatcher {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static enum CallType {

        SYNC,

        ASYNC
    }

    public static abstract class DataEventHandler implements IEventHandler {

        private CountDownLatch latch = null;

        private InfrastructureManager infraManager;

        private IEventDispatcher dispatcher;

        private boolean removeAfterAwait;

        public abstract void handleDataEvent(final int srcInstanceID, final Object value);

        @Override
        public void handleEvent(final Event e) {
            final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
            final int srcInstanceID = infraManager.getInstanceIDFromMachineUID(event.srcMachineID);
            handleDataEvent(srcInstanceID, event.getPayload());
            latch.countDown();

            if (removeAfterAwait && latch.getCount() == 0)
                dispatcher.removeEventListener(event.type, this);
        }

        public void initLatch(final int n) { latch = new CountDownLatch(n); }

        public CountDownLatch getLatch() { return latch; }

        public void setDispatcher(final IEventDispatcher dispatcher) { this.dispatcher = dispatcher; }

        public void setInfraManager(final InfrastructureManager infraManager) { this.infraManager = infraManager; }

        public void setRemoveAfterAwait(final boolean removeAfterAwait) { this.removeAfterAwait = removeAfterAwait; }
    }

    public static interface PullRequestHandler {

        public abstract Object handlePullRequest(final String name);
    }

    public interface Merger<T extends MObject> {

        public abstract void merge(final T dst, final List<T> src);
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String BSP_SYNC_BARRIER_EVENT = "bsp_sync_barrier_event";

    private static final String PUSH_EVENT_PREFIX = "push__";

    private static final String PULL_EVENT_PREFIX = "pull__";

    //private static final String PSERVER_LFSM_COMPUTED_FILE_SPLITS  = "PSERVER_LFSM_COMPUTED_FILE_SPLITS";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    private final IConfig config;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final FileSystemManager fileSystemManager;

    private final DHT dht;

    private final int instanceID;

    private final Map<String, MatrixLoadTask> matrixLoadTasks;

    private final Map<String, AtomicInteger> fileLoadingSyncBarrier;

    private CountDownLatch finishedLoadingLatch;

    private final Map<String, Matrix> loadingMatrices;

    private final Map<UUID, List<Serializable>> resultObjects;

    private final Map<Long, PServerContext> contextResolver;

    // ---------------------------------------------------

    private final int[] instanceIDs;

    private final int[] remoteInstanceIDs;

    private CountDownLatch bspSyncBarrier;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataManager(final IConfig config,
                       final InfrastructureManager infraManager,
                       final NetManager netManager,
                       final FileSystemManager fileSystemManager,
                       final DHT dht) {
        super(true);

        this.config = Preconditions.checkNotNull(config);
        this.infraManager = Preconditions.checkNotNull(infraManager);
        this.netManager = Preconditions.checkNotNull(netManager);
        this.fileSystemManager = fileSystemManager;
        this.dht = Preconditions.checkNotNull(dht);
        this.instanceID = infraManager.getInstanceID();
        this.matrixLoadTasks = new HashMap<>();
        this.resultObjects = new HashMap<>();
        this.contextResolver = new ConcurrentHashMap<>();

        fileLoadingSyncBarrier = new HashMap<>();
        this.bspSyncBarrier = new CountDownLatch(infraManager.getMachines().size() - 1);

        loadingMatrices = new HashMap<>();

        this.netManager.addEventListener(BSP_SYNC_BARRIER_EVENT, event -> bspSyncBarrier.countDown());
        this.netManager.addEventListener(Events.MATRIX_ENTRY_PARTITION_EVENT, new MatrixEntryPartitionEventHandler());
        this.netManager.addEventListener(Events.FINISHED_LOADING_FILE_EVENT, event -> {
            FinishedLoadingFileEvent e = Preconditions.checkNotNull((FinishedLoadingFileEvent) event);
            instanceFinishedProcessingSplit(e.getName());
        });

        this.instanceIDs = IntStream.iterate(0, x -> x + 1).limit(infraManager.getMachines().size()).toArray();

        int numOfRemoteWorkers = infraManager.getMachines().size() - 1;
        this.remoteInstanceIDs = new int[numOfRemoteWorkers];
        int i = 0, j = 0;
        for (MachineDescriptor md : infraManager.getMachines()) {
            if (!md.equals(infraManager.getMachine())) {
                remoteInstanceIDs[j] = i;
                ++j;
            }
            ++i;
        }
    }

    // ---------------------------------------------------
    // DATA LOADING
    // ---------------------------------------------------

    public void loadAsMatrix(final String filePath, long rows, long cols) {
        loadAsMatrix(filePath, rows, cols, RecordFormat.DEFAULT, Matrix.Format.DENSE_MATRIX, Matrix.Layout.ROW_LAYOUT,
                new MatrixByRowPartitioner(instanceID, instanceIDs.length, rows, cols));
    }

    public void loadAsMatrix(final String filePath, long rows, long cols, RecordFormat recordFormat) {
        loadAsMatrix(filePath, rows, cols, recordFormat, Matrix.Format.DENSE_MATRIX, Matrix.Layout.ROW_LAYOUT,
                new MatrixByRowPartitioner(instanceID, instanceIDs.length, rows, cols));
    }

    public void loadAsMatrix(final String filePath, long rows, long cols, RecordFormat recordFormat,
                             Matrix.Format matrixFormat, Matrix.Layout matrixLayout) {
        loadAsMatrix(filePath, rows, cols, recordFormat, matrixFormat, matrixLayout,
                new MatrixByRowPartitioner(instanceID, instanceIDs.length, rows, cols));
    }

    public void loadAsMatrix(final String filePath, long rows, long cols, RecordFormat recordFormat,
                             Matrix.Format matrixFormat, Matrix.Layout matrixLayout,
                             IMatrixPartitioner matrixPartitioner) {
        matrixLoadTasks.put(filePath, new MatrixLoadTask(filePath, recordFormat, rows, cols,
                matrixFormat, matrixLayout, matrixPartitioner));
        fileLoadingSyncBarrier.put(filePath, new AtomicInteger(instanceIDs.length));
    }

    // ---------------------------------------------------
    // EVENT HANDLING
    // ---------------------------------------------------

    public void addDataEventListener(final int n, final String name, final DataEventHandler handler) {
        handler.setInfraManager(infraManager);
        handler.initLatch(n);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, handler);
    }

    public void removeDataEventListener(final String name, final DataEventHandler handler) {
        netManager.removeEventListener(PUSH_EVENT_PREFIX + name, handler);
    }

    // ---------------------------------------------------
    // COMMUNICATION PRIMITIVES
    // ---------------------------------------------------

    public Value[] pullFrom(final String name, final int[] instanceIDs) {
        Preconditions.checkNotNull(name);
        int idx = 0;
        final Set<Key> keys = dht.getKey(name);
        Preconditions.checkState(instanceIDs.length <= keys.size());
        final Value[] values = new Value[instanceIDs.length];
        for (final int id : instanceIDs) {
            for (final Key key : keys) {
                if (key.getPartitionDescriptor(id) != null) {
                    values[idx] = dht.get(key)[0];
                    values[idx].setKey(key);
                    ++idx;
                    break;
                }
            }
        }
        return values;
    }

    public Value[] pullFromAll(final String name) {
        Preconditions.checkNotNull(name);
        int idx = 0;
        final Set<Key> keys = dht.getKey(name);
        final Value[] values = new Value[keys.size()];
        for (final Key key : keys) {
            values[idx] = dht.get(key)[0];
            values[idx].setKey(key);
            ++idx;
        }
        return values;
    }

    // ---------------------------------------------------

    public void pushTo(final String name, final Object value, final int[] instanceIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(instanceIDs);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(PUSH_EVENT_PREFIX + name, true);
        event.setPayload(value);
        netManager.sendEvent(instanceIDs, event);
    }

    public void pushTo(final String name, final Object value) {
        Preconditions.checkNotNull(name);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(PUSH_EVENT_PREFIX + name, true);
        event.setPayload(value);
        // remote instances.
        netManager.sendEvent(remoteInstanceIDs, event);
        // local instance.
        netManager.dispatchEvent(event);
    }

    public void awaitEvent(final CallType type, final String name, final DataEventHandler handler) {
        awaitEvent(type, remoteInstanceIDs.length, name, handler); }
    public void awaitEvent(final CallType type, final int n, final String name, final DataEventHandler handler) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        handler.setDispatcher(netManager);
        handler.setInfraManager(infraManager);
        handler.setRemoveAfterAwait(true);
        handler.initLatch(n);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, handler);
        if (type == CallType.SYNC) {
            try {
                handler.getLatch().await();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
            }
        }
    }

    // ---------------------------------------------------

    public void registerPullRequestHandler(final String name, final PullRequestHandler handler) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        final DataManager self = this;
        netManager.addEventListener(PULL_EVENT_PREFIX + name, e -> {
            final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
            final int srcInstanceID = infraManager.getInstanceIDFromMachineUID(event.srcMachineID);
            final Object result = handler.handlePullRequest(name);
            self.pushTo(name, result, new int[]{srcInstanceID});
        });
    }

    public Object[] pullRequest(final String name) { return pullRequest(name, remoteInstanceIDs); }
    public Object[] pullRequest(final String name, final int[] instanceIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(instanceIDs);

        final Object[] pullResponses = new Object[instanceIDs.length];
        final AtomicInteger responseCounter = new AtomicInteger(0);

        final DataEventHandler responseHandler = new DataEventHandler() {
            @Override
            public void handleDataEvent(int srcInstanceID, final Object value) {
                pullResponses[responseCounter.getAndIncrement()] = value;
            }
        };

        responseHandler.setDispatcher(netManager);
        responseHandler.setInfraManager(infraManager);
        responseHandler.setRemoveAfterAwait(true);
        responseHandler.initLatch(instanceIDs.length);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, responseHandler);

        // send pull request to all instances.
        NetEvents.NetEvent event = new NetEvents.NetEvent(PULL_EVENT_PREFIX + name, true);
        netManager.sendEvent(instanceIDs, event);

        try {
            responseHandler.getLatch().await();
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
        }

        return pullResponses;
    }

    // ---------------------------------------------------
    // OBJECT MANAGEMENT
    // ---------------------------------------------------

    public Key putLocal(final String name, final AbstractBufferValue value) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(value);
        final Key key = dht.createLocalKey(name);
        value.setValueMetadata(instanceID);
        dht.put(key, value);
        return key;
    }

    public Value[] getLocal(final String name) {
        Preconditions.checkNotNull(name);
        Key localKey = null;
        final Set<Key> keys = dht.getKey(name);
        for (final Key k : keys) {
            if (k.getPartitionDescriptor(instanceID) != null) {
                localKey = k;
                break;
            }
        }
        return getLocal(localKey);
    }

    public Value[] getLocal(final Key key) {
        Preconditions.checkNotNull(key);
        return dht.get(key);
    }

    public void removeLocal(final String name) {
        Preconditions.checkNotNull(name);
        Key localKey = null;
        final Set<Key> keys = dht.getKey(name);
        for (final Key k : keys) {
            if (k.getPartitionDescriptor(instanceID) != null) {
                localKey = k;
                break;
            }
        }
        removeLocal(localKey);
    }

    public void removeLocal(final Key key) {
        Preconditions.checkNotNull(key);
        dht.delete(key);
    }

    // ---------------------------------------------------

    public <T extends MObject> Key putObject(final String name, final T obj) {
        return putLocal(name, new MObjectValue<T>(obj));
    }

    public <T extends MObject> T getObject(final String name) {
        return (T) ((MObjectValue) getLocal(name)[0]).object;
    }

    // ---------------------------------------------------


    public <T extends MObject> void pullMerge(final T dstObj,
                                              final Merger<T> merger) {

        pullMerge(((MObjectValue<T>) dstObj.getOwner()).getKey().name, instanceIDs, dstObj, merger);
    }


    public <T extends MObject> void pullMerge(final String name,
                                              final T dstObj,
                                              final Merger<T> merger) {

        pullMerge(name, instanceIDs, dstObj, merger);
    }

    public <T extends MObject> void pullMerge(final String name,
                                              final int[] instanceIDs,
                                              final T dstObj,
                                              final Merger<T> merger) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(instanceIDs);
        Preconditions.checkNotNull(dstObj);
        Preconditions.checkNotNull(merger);

        final Value[] values = pullFrom(name, instanceIDs);
        if (values.length > 0) {
            //if (values.getClass().getComponentType() != dstObj.getClass())
            //    throw new IllegalStateException();
            final List<Value> valueList = Arrays.asList(values);
            Collections.sort(valueList,
                    (Value o1, Value o2) -> ((Integer) o1.getValueMetadata()).compareTo(((Integer) o2.getValueMetadata())));
            final List<T> mObjects = valueList.stream().map(v -> ((MObjectValue<T>) v).object).collect(Collectors.toList());
            merger.merge(dstObj, mObjects);
        } else
            throw new IllegalStateException();
    }

    // ---------------------------------------------------
    // CONTROL FLOW
    // ---------------------------------------------------

    public void sync() {
        netManager.broadcastEvent(new NetEvents.NetEvent(BSP_SYNC_BARRIER_EVENT));
        try {
            bspSyncBarrier.await();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
        if (bspSyncBarrier.getCount() == 0) {
            bspSyncBarrier = new CountDownLatch(infraManager.getMachines().size() - 1);
        } else {
            throw new IllegalStateException();
        }
    }

    // ---------------------------------------------------
    // THREAD PARALLEL PRIMITIVES
    // ---------------------------------------------------

    public Matrix.RowIterator createThreadPartitionedRowIterator(final Matrix matrix) {
        Preconditions.checkNotNull(matrix);
        final long systemThreadID = Thread.currentThread().getId();
        final PServerContext ctx = contextResolver.get(systemThreadID);
        final int rowBlock = (int) matrix.numRows() / ctx.perNodeParallelism;
        int end = (ctx.threadID * rowBlock + rowBlock - 1);
        end = (ctx.threadID == ctx.perNodeParallelism - 1) ? end + (int) matrix.numRows() % ctx.perNodeParallelism : end;
        return matrix.rowIterator(ctx.threadID * rowBlock, end);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IConfig getConfig() {
        return config;
    }

    public DHT getDHT() {
        return dht;
    }

    // Must be called from the specific execution context!
    public void registerJobContext(final PServerContext ctx) {
        contextResolver.put(Thread.currentThread().getId(), Preconditions.checkNotNull(ctx));
    }

    public void setResults(final UUID jobUID, final List<Serializable> results) {
        Preconditions.checkNotNull(jobUID);
        Preconditions.checkNotNull(results);
        resultObjects.put(jobUID, results);
    }

    public List<Serializable> getResults(final UUID jobUID) {
        Preconditions.checkNotNull(jobUID);
        return resultObjects.get(jobUID);
    }

    public PServerContext getJobContext() {
        return contextResolver.get(Thread.currentThread().getId());
    }

    public int getInstanceID() { return instanceID; }

    public int[] getRemoteInstanceIDs() { return remoteInstanceIDs; }

    public int getNumberOfInstances() { return instanceIDs.length; }

    // ---------------------------------------------------

    public void postProloguePhase(final PServerContext ctx) {
        Preconditions.checkNotNull(ctx);
        if (fileSystemManager != null) {
            if (ctx.threadID == 0) {
                fileSystemManager.computeInputSplitsForRegisteredFiles();
                loadFilesIntoDHT();
            }
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void loadFilesIntoDHT() {
        finishedLoadingLatch = new CountDownLatch(matrixLoadTasks.size());
        // prepare to read entries that belong to foreign matrix partitions
        Map<Integer, List<MatrixEntry>> foreignEntries = new HashMap<Integer, List<MatrixEntry>>(); // data structure to hold foreign entries
        int foreignEntriesThreshold = 2048; // threshold that indicates how many entries are gathered before sending
        // iterate through load tasks
        for (final MatrixLoadTask task : matrixLoadTasks.values()) {
            // preallocate local matrix partition
            Matrix matrix = getLoadingMatrix(task);
            // iterate through records in file
            FileDataIterator<? extends IRecord> fileIterator = task.fileIterator;
            Matrix.PartitionShape partitionShape = task.matrixPartitioner.getPartitionShape();
            while (fileIterator.hasNext()) {
                final IRecord record = fileIterator.next();
                // iterate through entries in record
                ReusableMatrixEntry reusable = new MutableMatrixEntry(-1, -1, Double.NaN);
                synchronized (matrix) {
                    while (record.hasNext()) {
                        MatrixEntry entry = record.next(reusable);
                        //System.out.println(instanceID + ": " + entry);
                        // get the partition this record belongs to
                        int targetPartition = task.matrixPartitioner.getPartitionOfEntry(entry);
                        // if record belongs to own instance, set the value
                        if (targetPartition == instanceID) {
                            // set entry
                            // HOTFIX: partition aware matrix
                            matrix.set(entry.getRow() % partitionShape.getRows(), entry.getCol() % partitionShape.getCols(), entry.getValue());
                            //matrix.set(entry.getRow(), entry.getCol(), entry.getValue());
                        }
                        // otherwise append entry to foreign entries and send them depending on threshold
                        else {
                            List<MatrixEntry> foreignsOfTargetInstance = getSavely(foreignEntries, targetPartition, foreignEntriesThreshold);
                            foreignsOfTargetInstance.add(new ImmutableMatrixEntry(entry));
                            if (foreignsOfTargetInstance.size() >= foreignEntriesThreshold) {
                                // send them
                                sendPartition(targetPartition, foreignsOfTargetInstance, task);
                            }
                        } // </partition check>
                    } // </entries in record iteration
                }
            } // </records in file iteration>>
            // send all remaining foreign entries
            for (Map.Entry<Integer, List<MatrixEntry>> map : foreignEntries.entrySet()) {
                sendPartition(map.getKey(), map.getValue(), task);
            }
            netManager.broadcastEvent(new FinishedLoadingFileEvent(fileIterator.getFilePath()));
            instanceFinishedProcessingSplit(fileIterator.getFilePath());
        }
        try {
            finishedLoadingLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Is called whenever an instance finished processing an input split. This is triggered either by reaching the end
     * of the own input split or by receiving a @link FinishedLoadingFileEvent. If all instances finished processing,
     * the matrix can be put into the DHT.
     *
     * @param name
     */
    private void instanceFinishedProcessingSplit(String name) {
        int counter;
        synchronized (fileLoadingSyncBarrier) {
            counter = fileLoadingSyncBarrier.get(name).decrementAndGet();
        }
        if (counter <= 0) {
            // is it possible that a FinishedLoading event overtakes a SendPartition event?
            // this assumes it is not:
            Matrix matrix;
            synchronized (loadingMatrices) {
                matrix = Preconditions.checkNotNull(loadingMatrices.get(name));
            }
            synchronized (matrix) {
                putObject(name, matrix);
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

    private void sendPartition(int targetInstanceId, List<MatrixEntry> entries, MatrixLoadTask task) {
        if (entries != null && !entries.isEmpty()) {
            MatrixEntry[] entriesArray = entries.toArray(new MatrixEntry[entries.size()]);
            netManager.sendEvent(targetInstanceId, new MatrixEntryPartitionEvent(entriesArray, task.fileIterator.getFilePath()));
            entries.clear();
        }
    }

    private Matrix getLoadingMatrix(MatrixLoadTask task) {
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

    private class MatrixLoadTask {

        final FileDataIterator fileIterator;
        final RecordFormat recordFormat;
        final long rows;
        final long cols;
        final Matrix.Format matrixFormat;
        final Matrix.Layout matrixLayout;
        final IMatrixPartitioner matrixPartitioner;

        public MatrixLoadTask(String filePath, RecordFormat recordFormat, long rows, long cols, Matrix.Format matrixFormat, Matrix.Layout matrixLayout, IMatrixPartitioner matrixPartitioner) {
            this.fileIterator = fileSystemManager.createFileIterator(filePath, recordFormat);
            this.recordFormat = recordFormat;
            this.rows = rows;
            this.cols = cols;
            this.matrixFormat = matrixFormat;
            this.matrixLayout = matrixLayout;
            this.matrixPartitioner = matrixPartitioner;
        }
    }

    // -----------------------------------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------------------------------

    public static final class Events {

        public static final String FINISHED_LOADING_FILE_EVENT = "FINISHED_LOADING_FILE_EVENT";

        public static final String MATRIX_ENTRY_PARTITION_EVENT = "MATRIX_ENTRY_PARTITION_EVENT";

    }

    /**
     * Is send from an instance, that loads "foreign" matrix entries from its input files.<br>
     * From the perspective of one instance, foreign matrix entries are those belonging to another instance.<br>
     * Received from an instance, to that the containing entries belong to.
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
            MatrixEntryPartitionEvent e = Preconditions.checkNotNull((MatrixEntryPartitionEvent) event);
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
}
