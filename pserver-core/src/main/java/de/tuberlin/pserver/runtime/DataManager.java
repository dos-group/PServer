package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.ResettableCountDownLatch;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.dht.DHTObject;
import de.tuberlin.pserver.runtime.dht.types.AbstractBufferedDHTObject;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.config.AbstractRecordFormatConfig;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.MatrixPartitionManager;
import de.tuberlin.pserver.types.PartitionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataManager extends EventDispatcher {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static abstract class DataEventHandler implements IEventHandler {

        private ResettableCountDownLatch latch = null;

        private InfrastructureManager infraManager;

        private IEventDispatcher dispatcher;

        private boolean removeAfterAwait;

        public abstract void handleDataEvent(final int srcNodeID, final Object value);

        @Override
        public void handleEvent(final Event e) {
            final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
            final int srcNodeID = infraManager.getNodeIDFromMachineUID(event.srcMachineID);
            handleDataEvent(srcNodeID, event.getPayload());
            latch.countDown();

            if (removeAfterAwait && latch.getCount() == 0)
                dispatcher.removeEventListener(event.type, this);
        }

        public void initLatch(final int n) { latch = new ResettableCountDownLatch(n); }

        public void reset() { latch.reset(); }

        public ResettableCountDownLatch getLatch() { return latch; }

        public void setDispatcher(final IEventDispatcher dispatcher) { this.dispatcher = dispatcher; }

        public void setInfraManager(final InfrastructureManager infraManager) { this.infraManager = infraManager; }

        public void setRemoveAfterAwait(final boolean removeAfterAwait) { this.removeAfterAwait = removeAfterAwait; }
    }

    // ---------------------------------------------------

    public static interface PullRequestHandler {

        public abstract Object handlePullRequest(final String name);
    }

    // ---------------------------------------------------

    public interface Merger<T extends SharedObject> {

        public abstract void merge(final T dst, final List<T> src);
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String PUSH_EVENT_PREFIX = "push__";

    private static final String PULL_EVENT_PREFIX = "pull__";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final ExecutionManager executionManager;

    private final FileSystemManager fileSystemManager;

    private final MatrixPartitionManager matrixPartitionManager;

    private final DHTManager dht;

    private final int nodeID;

    private final Map<UUID, List<Serializable>> resultObjects;

    // ---------------------------------------------------

    public final int[] nodeIDs;

    public final int[] remoteNodeIDs;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataManager(final IConfig config,
                       final InfrastructureManager infraManager,
                       final NetManager netManager,
                       final ExecutionManager executionManager,
                       final FileSystemManager fileSystemManager,
                       final DHTManager dht) {
        super(true);

        this.infraManager       = Preconditions.checkNotNull(infraManager);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.executionManager   = Preconditions.checkNotNull(executionManager);
        this.fileSystemManager  = fileSystemManager;
        this.dht                = Preconditions.checkNotNull(dht);
        this.nodeID             = infraManager.getNodeID();
        this.resultObjects      = new HashMap<>();
        this.matrixPartitionManager = new MatrixPartitionManager(netManager, fileSystemManager, this);

        this.nodeIDs = IntStream.iterate(0, x -> x + 1).limit(infraManager.getMachines().size()).toArray();
        int numOfRemoteWorkers = infraManager.getMachines().size() - 1;
        this.remoteNodeIDs = new int[numOfRemoteWorkers];
        int i = 0, j = 0;
        for (MachineDescriptor md : infraManager.getMachines()) {
            if (!md.equals(infraManager.getMachine())) {
                remoteNodeIDs[j] = i;
                ++j;
            }
            ++i;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void clearContext() {
        matrixPartitionManager.clearContext();
        fileSystemManager.clearContext();
    }

    // ---------------------------------------------------
    // EVENT HANDLING
    // ---------------------------------------------------

    public void addDataEventListener(final String name, final DataEventHandler handler) {
        addDataEventListener(remoteNodeIDs.length, name, handler);
    }
    public void addDataEventListener(final int n, final String name, final DataEventHandler handler) {
        handler.setInfraManager(infraManager);
        handler.initLatch(n);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, handler);
    }

    public void removeDataEventListener(final String name, final DataEventHandler handler) {
        netManager.removeEventListener(PUSH_EVENT_PREFIX + name, handler);
    }

    // ---------------------------------------------------
    // DATA LOADING
    // ---------------------------------------------------

    public Matrix loadAsMatrix(final SlotContext slotContext,
                             final String filePath,
                             final String name,
                             final long rows,
                             final long cols,
                             final GlobalScope globalScope,
                             final Class<? extends IMatrixPartitioner> partitionerClass,
                             final AbstractRecordFormatConfig recordFormat,
                             final Format matrixFormat,
                             final Layout matrixLayout) {

        return matrixPartitionManager.load(
                slotContext,
                filePath,
                name,
                rows,
                cols,
                globalScope,
                partitionerClass,
                recordFormat,
                matrixFormat,
                matrixLayout
        );
    }

    // ---------------------------------------------------
    // COMMUNICATION PRIMITIVES
    // ---------------------------------------------------

    public DHTObject[] pullFrom(final String name, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        int idx = 0;
        final Set<DHTKey> keys = dht.getKey(name);
        Preconditions.checkState(nodeIDs.length <= keys.size());
        final DHTObject[] dhtObjects = new DHTObject[nodeIDs.length];
        for (final int id : nodeIDs) {
            for (final DHTKey key : keys) {
                if (key.getPartitionDescriptor(id) != null) {
                    dhtObjects[idx] = dht.get(key)[0];
                    dhtObjects[idx].setKey(key);
                    ++idx;
                    break;
                }
            }
        }
        return dhtObjects;
    }

    public DHTObject[] pullFromAll(final String name) {
        Preconditions.checkNotNull(name);
        int idx = 0;
        final Set<DHTKey> keys = dht.getKey(name);
        final DHTObject[] dhtObjects = new DHTObject[keys.size()];
        for (final DHTKey key : keys) {
            dhtObjects[idx] = dht.get(key)[0];
            dhtObjects[idx].setKey(key);
            ++idx;
        }
        return dhtObjects;
    }

    // ---------------------------------------------------

    public synchronized void pushTo(final String name, final Object value, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(PUSH_EVENT_PREFIX + name, true);
        event.setPayload(value);
        netManager.sendEvent(nodeIDs, event);
    }

    public synchronized void pushTo(final String name, final Object value) {
        Preconditions.checkNotNull(name);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(PUSH_EVENT_PREFIX + name, true);
        event.setPayload(value);
        // remote nodes.
        netManager.sendEvent(remoteNodeIDs, event);
        // local nodes.
        //netManager.dispatchEvent(event);
    }

    /*public void awaitEvent(final ExecutionManager.CallType type, final String name, final DataEventHandler handler) {
        awaitEvent(type, remoteNodeIDs.length, name, handler); }
    public void awaitEvent(final ExecutionManager.CallType type, final int n, final String name, final DataEventHandler handler) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        handler.setDispatcher(netManager);
        handler.setInfraManager(infraManager);
        handler.setRemoveAfterAwait(true);
        handler.initLatch(n);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, handler);
        if (type == ExecutionManager.CallType.SYNC) {
            try {
                handler.getLatch().await();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
            }
        }
    }*/

    //public void awaitEvent(final ExecutionManager.CallType type, final String name, final DataEventHandler handler) {
    //    awaitEvent(type, remoteNodeIDs.length, name, handler); }

    public void awaitEvent(final ExecutionManager.CallType type, final int n, final String name, final DataEventHandler handler) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        handler.setDispatcher(netManager);
        handler.setInfraManager(infraManager);
        handler.setRemoveAfterAwait(true);
        handler.initLatch(n);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, handler);
        if (type == ExecutionManager.CallType.SYNC) {
            try {
                handler.getLatch().await();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
            }
        }
    }

    public void registerPullRequestHandler(final String name, final PullRequestHandler handler) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(handler);
        final DataManager self = this;
        netManager.addEventListener(PULL_EVENT_PREFIX + name, e -> {
            final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
            final int srcNodeID = infraManager.getNodeIDFromMachineUID(event.srcMachineID);
            final Object result = handler.handlePullRequest(name);
            self.pushTo(name, result, new int[]{srcNodeID});
        });
    }

    public Object[] pullRequest(final String name) { return pullRequest(name, remoteNodeIDs); }
    public Object[] pullRequest(final String name, final int[] nodeIDs) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);

        final Object[] pullResponses = new Object[nodeIDs.length];
        final AtomicInteger responseCounter = new AtomicInteger(0);

        final DataEventHandler responseHandler = new DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, final Object value) {
                pullResponses[responseCounter.getAndIncrement()] = value;
            }
        };

        responseHandler.setDispatcher(netManager);
        responseHandler.setInfraManager(infraManager);
        responseHandler.setRemoveAfterAwait(true);
        responseHandler.initLatch(nodeIDs.length);
        netManager.addEventListener(PUSH_EVENT_PREFIX + name, responseHandler);

        // send pull request to all nodes.
        NetEvents.NetEvent event = new NetEvents.NetEvent(PULL_EVENT_PREFIX + name, true);
        netManager.sendEvent(nodeIDs, event);

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

    public DHTKey putLocal(final String name, final AbstractBufferedDHTObject value) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(value);
        final DHTKey key = dht.createLocalKey(name);
        value.setValueMetadata(nodeID);
        dht.put(key, value);
        return key;
    }

    public DHTObject[] getLocal(final String name) {
        Preconditions.checkNotNull(name);
        DHTKey localKey = null;
        final Set<DHTKey> key = dht.getKey(name);
        for (final DHTKey k : key) {
            if (k.getPartitionDescriptor(nodeID) != null) {
                localKey = k;
                break;
            }
        }
        return getLocal(localKey);
    }

    public DHTObject[] getLocal(final DHTKey key) {
        Preconditions.checkNotNull(key);
        return dht.get(key);
    }

    public void removeLocal(final String name) {
        Preconditions.checkNotNull(name);
        DHTKey localKey = null;
        final Set<DHTKey> key = dht.getKey(name);
        for (final DHTKey k : key) {
            if (k.getPartitionDescriptor(nodeID) != null) {
                localKey = k;
                break;
            }
        }
        removeLocal(localKey);
    }

    public void removeLocal(final DHTKey key) {
        Preconditions.checkNotNull(key);
        dht.delete(key);
    }

    // ---------------------------------------------------

    public <T extends SharedObject> DHTKey putObject(final String name, final EmbeddedDHTObject<T> embeddedObj) {
        return putLocal(name, embeddedObj);
    }

    public <T extends SharedObject> DHTKey putObject(final String name, final T obj) {
        return putLocal(name, new EmbeddedDHTObject<T>(obj));
    }

    @SuppressWarnings("unchecked")
    public <T extends SharedObject> T getObject(final String name) {
        return (T) ((EmbeddedDHTObject) getLocal(name)[0]).object;
    }

    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public <T extends SharedObject> void pullMerge(final T dstObj, final Merger<T> merger) {
        pullMerge(((EmbeddedDHTObject<T>) dstObj.getOwner()).getKey().name, nodeIDs, dstObj, merger);
    }

    public <T extends SharedObject> void pullMerge(final String name, final T dstObj, final Merger<T> merger) {
        pullMerge(name, nodeIDs, dstObj, merger);
    }

    public <T extends SharedObject> void pullMerge(final String name,
                                              final int[] nodeIDs,
                                              final T dstObj,
                                              final Merger<T> merger) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(nodeIDs);
        Preconditions.checkNotNull(dstObj);
        Preconditions.checkNotNull(merger);

        final DHTObject[] dhtObjects = pullFrom(name, nodeIDs);
        if (dhtObjects.length > 0) {
            //if (values.getClass().getComponentType() != dstObj.getClass())
            //    throw new IllegalStateException();
            final List<DHTObject> dhtObjectList = Arrays.asList(dhtObjects);
            Collections.sort(dhtObjectList,
                    (DHTObject o1, DHTObject o2) -> ((Integer) o1.getValueMetadata()).compareTo(((Integer) o2.getValueMetadata())));
            @SuppressWarnings("unchecked")
            final List<T> mObjects = dhtObjectList.stream().map(v -> ((EmbeddedDHTObject<T>) v).object).collect(Collectors.toList());
            merger.merge(dstObj, mObjects);
        } else
            throw new IllegalStateException();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setResults(final UUID jobUID, final List<Serializable> results) {
        Preconditions.checkNotNull(jobUID);
        Preconditions.checkNotNull(results);
        resultObjects.put(jobUID, results);
    }

    public List<Serializable> getResults(final UUID jobUID) {
        Preconditions.checkNotNull(jobUID);
        return resultObjects.get(jobUID);
    }

    public void loadInputData(final SlotContext ctx) throws Exception{
        Preconditions.checkNotNull(ctx);
        Preconditions.checkNotNull(fileSystemManager);
        if (ctx.slotID == 0) {
            matrixPartitionManager.loadFilesIntoDHT();
        }
    }
}
