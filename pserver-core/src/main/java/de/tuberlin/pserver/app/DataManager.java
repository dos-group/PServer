package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.app.dht.valuetypes.AbstractBufferValue;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.FileSystemManager;
import de.tuberlin.pserver.app.filesystem.record.IRecord;
import de.tuberlin.pserver.app.types.MObjectValue;
import de.tuberlin.pserver.app.types.*;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.MObject;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.MatrixBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataManager extends EventDispatcher {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public abstract class DataEventHandler implements IEventHandler {

        public abstract void handleDataEvent(final int srcInstanceID, final Value[] values);

        @Override
        public void handleEvent(final Event e) {
            final NetEvents.NetEvent event = (NetEvents.NetEvent)e;
            final int srcInstanceID = infraManager.getInstanceIDFromMachineUID(event.srcMachineID);
            final Value[] values = (Value[]) event.getPayload();
            handleDataEvent(srcInstanceID, values);
        }
    }

    public interface Merger<T extends MObject> {

        public abstract void merge(final T dst, final List<T> src);
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String BSP_SYNC_BARRIER_EVENT = "bsp_sync_barrier_event";

    private static final String PUSH_EVENT_PREFIX = "push__";

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

    private final List<FileDataIterator<? extends IRecord>> filesToLoad;

    private final Map<UUID, List<Serializable>> resultObjects;

    private final Map<Long, PServerContext> contextResolver;

    // ---------------------------------------------------

    private final int[] instanceIDs;

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

        this.config             = Preconditions.checkNotNull(config);
        this.infraManager       = Preconditions.checkNotNull(infraManager);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.fileSystemManager  = fileSystemManager;
        this.dht                = Preconditions.checkNotNull(dht);
        this.instanceID         = infraManager.getInstanceID();
        this.filesToLoad        = new ArrayList<>();
        this.resultObjects      = new HashMap<>();
        this.contextResolver    = new ConcurrentHashMap<>();

        this.bspSyncBarrier = new CountDownLatch(infraManager.getMachines().size());
        this.netManager.addEventListener(BSP_SYNC_BARRIER_EVENT, event -> bspSyncBarrier.countDown());

        this.instanceIDs = IntStream.iterate(0, x -> x + 1).limit(infraManager.getMachines().size()).toArray();
    }

    // ---------------------------------------------------
    // DATA LOADING
    // ---------------------------------------------------

    public void loadAsMatrix(final String filePath) {
        filesToLoad.add(createFileIterator(Preconditions.checkNotNull(filePath), null));
    }

    // ---------------------------------------------------
    // EVENT HANDLING
    // ---------------------------------------------------

    public void addDataEventListener(final String name, final DataEventHandler handler) {
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

    public void pushTo(final Value[] values, final int[] instanceIDs) {
        Preconditions.checkNotNull(instanceIDs);
        Preconditions.checkNotNull(values);
        for (final int id : instanceIDs) {
            final NetEvents.NetEvent event = new NetEvents.NetEvent(PUSH_EVENT_PREFIX + values[0].getKey().name);
            event.setPayload(values);
            netManager.sendEvent(id, event);
        }
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

    public void pushToAll(final Value[] values) {
        Preconditions.checkNotNull(values);
        for (final MachineDescriptor md : infraManager.getMachines()) {
            final NetEvents.NetEvent event = new NetEvents.NetEvent(PUSH_EVENT_PREFIX + values[0].getKey().name);
            event.setPayload(values);
            netManager.sendEvent(md, event);
        }
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
        return (T)((MObjectValue)getLocal(name)[0]).object;
    }

    // ---------------------------------------------------


    public <T extends MObject> void pullMerge(final T dstObj,
                                              final Merger<T> merger) {

        pullMerge(((MObjectValue<T>)dstObj.getOwner()).getKey().name, instanceIDs, dstObj, merger);
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
            if (values.getClass().getComponentType() != dstObj.getClass())
                throw new IllegalStateException();
            final List<Value> valueList = Arrays.asList(values);
            Collections.sort(valueList,
                    (Value o1, Value o2) -> ((Integer)o1.getValueMetadata()).compareTo(((Integer)o2.getValueMetadata())));
            final List<T> mObjects = valueList.stream().map(v -> ((MObjectValue<T>)v).object).collect(Collectors.toList());
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
            bspSyncBarrier = new CountDownLatch(infraManager.getMachines().size());
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
        final int rowBlock = (int)matrix.numRows() / ctx.perNodeParallelism;
        int end = (ctx.threadID * rowBlock + rowBlock - 1);
        end = (ctx.threadID == ctx.perNodeParallelism - 1) ?  end + (int)matrix.numRows() % ctx.perNodeParallelism : end;
        return matrix.rowIterator(ctx.threadID * rowBlock, end);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IConfig getConfig() { return config; }

    public DHT getDHT() { return dht; }

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

    public <T extends IRecord> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType) {
        return fileSystemManager != null ? fileSystemManager.createFileIterator(filePath, recordType) : null;
    }

    private void loadFilesIntoDHT() {
        for (final FileDataIterator<? extends IRecord> fileIterator : filesToLoad) {
            double[] data = new double[0];
            double[] currentSegment = new double[4096];
            ReusableMatrixEntry reusable = new MutableMatrixEntry(0, 0, 0);
            int rows = 0, cols = -1, localIndex = 0;
            while (fileIterator.hasNext()) {

                final IRecord record = fileIterator.next();

                if (cols == -1)
                    cols = record.size();
                if (record.size() != cols)
                    throw new IllegalStateException("cols must always have length: " + cols + " but it has record.length = " + record.size());

                while(record.hasNext()) {
                    if (localIndex == currentSegment.length - 1) {
                        data = ArrayUtils.addAll(data, currentSegment);
                        currentSegment = new double[4096];
                        localIndex = 0;
                    }
                    MatrixEntry entry = record.next(reusable);
                    currentSegment[localIndex] = entry.getValue();
                    ++localIndex;
                }

                for (int i = 0; i < record.size(); ++i) {

                }
                ++rows;
            }
            if (localIndex > 0)
                data = ArrayUtils.addAll(data, currentSegment); // TODO: We waste here a bit memory, if the last segment is not full...

            final String filename = Paths.get(fileIterator.getFilePath()).getFileName().toString();

            Matrix dataMatrix = new MatrixBuilder()
                    .dimension(rows, cols)
                    .format(Matrix.Format.DENSE_MATRIX)
                    .layout(Matrix.Layout.ROW_LAYOUT)
                    .data(data)
                    .build();

            putObject(filename, dataMatrix);
        }
    }
}
