package de.tuberlin.pserver.runtime.dht;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.commons.hashtable.NonBlockingHashMap;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.runtime.core.events.EventDispatcher;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.lifecycle.Deactivatable;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.dht.types.AbstractBufferedDHTObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class DHTManager extends EventDispatcher implements Deactivatable {

    // ---------------------------------------------------
    // DHT Events.
    // ---------------------------------------------------

    public static final String DHT_EVENT_PUT_VALUE                  = "dht_event_put_value";

    public static final String DHT_EVENT_PUT_SEGMENTS               = "dht_event_put_segments";

    public static final String DHT_EVENT_GET_VALUE_REQUEST          = "dht_event_get_value_request";

    public static final String DHT_EVENT_GET_VALUE_RESPONSE         = "dht_event_get_value_response";

    public static final String DHT_EVENT_GET_SEGMENTS_REQUEST       = "dht_event_get_segments_request";

    public static final String DHT_EVENT_GET_SEGMENTS_RESPONSE      = "dht_event_get_segments_response";

    public static final String DHT_EVENT_DELETE                     = "dht_event_delete";

    public static final String DHT_EVENT_ADD_KEY_TO_DIRECTORY       = "dht_event_add_key_to_directory";

    public static final String DHT_EVENT_REMOVE_KEY_FROM_DIRECTORY  = "dht_event_remove_key_from_directory";

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class GlobalKeyDirectory {

        // ---------------------------------------------------

        private final class DHTAddKeyToDirectoryHandler implements IEventHandler {
            @Override public void handleEvent(final Event e) { put((DHTKey)e.getPayload()); }
        }

        private final class DHTRemoveKeyToDirectoryHandler implements IEventHandler {
            @Override public void handleEvent(final Event e) { remove((DHTKey)e.getPayload()); }
        }

        // ---------------------------------------------------

        private final Map<UUID, DHTKey> uidKeyDirectory;

        private final Map<String,Map<UUID,DHTKey>> keyDirectory;

        // ---------------------------------------------------

        public GlobalKeyDirectory() {
            this.uidKeyDirectory = new NonBlockingHashMap<>();
            this.keyDirectory = new NonBlockingHashMap<>();
            netManager.addEventListener(DHT_EVENT_ADD_KEY_TO_DIRECTORY, new DHTAddKeyToDirectoryHandler());
            netManager.addEventListener(DHT_EVENT_REMOVE_KEY_FROM_DIRECTORY, new DHTRemoveKeyToDirectoryHandler());
        }

        // ---------------------------------------------------

        public void put(final DHTKey key) {
            Map<UUID,DHTKey> keys = keyDirectory.get(Preconditions.checkNotNull(key.name));
            if (keys == null) {
                keys = new NonBlockingHashMap<>();
                keyDirectory.put(key.name, keys);
            }
            keys.put(key.internalUID, key);
            uidKeyDirectory.put(key.internalUID, key);
        }

        public void globalPut(final DHTKey key) {
            put(key);
            final NetEvent event = new NetEvent(DHT_EVENT_ADD_KEY_TO_DIRECTORY);
            event.setPayload(key);
            netManager.broadcastEvent(event);
        }

        public Set<DHTKey> get(final String name) { return new HashSet<>(keyDirectory.get(name).values()); }

        public DHTKey get(final UUID uid) { return uidKeyDirectory.get(uid); }

        public void remove(final DHTKey key) {
            final Map<UUID,DHTKey> keys = keyDirectory.get(Preconditions.checkNotNull(key.name));
            Preconditions.checkState(keys != null);
            final DHTKey k = keys.get(key.internalUID);
            Preconditions.checkState(k != null);
            keys.remove(k);
        }

        public void globalRemove(final DHTKey key) {
            remove(key);
            final NetEvent event = new NetEvent(DHT_EVENT_REMOVE_KEY_FROM_DIRECTORY);
            event.setPayload(key);
            netManager.broadcastEvent(event);
        }

        public void clearContext() {
            keyDirectory.clear();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DHTManager.class);

    private static long RESPONSE_TIMEOUT = 55000; // in ms

    private static final Object globalDHTMutex = new Object();

    //private static final AtomicReference<DHTManager> globalDHTInstance = new AtomicReference<>(null);

    // ---------------------------------------------------

    private final Config config;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final Compressor.CompressionType compressionType;

    private final Map<DHTKey,AbstractBufferedDHTObject> store;

    //private final Map<DHTKey,DHTObject> lstore;

    // ---------------------------------------

    private final int nodeID;

    private final GlobalKeyDirectory globalKeyDirectory;

    // ---------------------------------------

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final Map<UUID,CountDownLatch> requestTable = new NonBlockingHashMap<>();

    private final Map<UUID,AbstractBufferedDHTObject> responseValueTable = new NonBlockingHashMap<>();

    private final Map<UUID,AbstractBufferedDHTObject.Segment[]> responseSegmentsTable = new NonBlockingHashMap<>();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DHTManager(final Config config,
                      final InfrastructureManager infraManager,
                      final NetManager netManager) {

        super(true, "DHT-THREAD");

        //synchronized (globalDHTMutex) {
        //    if (!globalDHTInstance.compareAndSet(null, this))
        //        throw new IllegalStateException();
        //}

        this.config         = Preconditions.checkNotNull(config);
        this.infraManager   = Preconditions.checkNotNull(infraManager);
        this.netManager     = Preconditions.checkNotNull(netManager);
        this.nodeID         = infraManager.getNodeID();
        this.store          = new NonBlockingHashMap<>();
        //this.lstore         = new NonBlockingHashMap<>();

        this.compressionType = Compressor.CompressionType.NO_COMPRESSION;

        // Register DHT events.
        netManager.addEventListener(DHT_EVENT_PUT_VALUE, new DHTPutValueHandler());
        netManager.addEventListener(DHT_EVENT_PUT_SEGMENTS, new DHTPutSegmentsHandler());
        netManager.addEventListener(DHT_EVENT_GET_VALUE_REQUEST, new DHTGetValueRequestHandler());
        netManager.addEventListener(DHT_EVENT_GET_VALUE_RESPONSE, new DHTGetValueResponseHandler());
        netManager.addEventListener(DHT_EVENT_GET_SEGMENTS_REQUEST, new DHTGetSegmentsRequestHandler());
        netManager.addEventListener(DHT_EVENT_GET_SEGMENTS_RESPONSE, new DHTGetSegmentsResponseHandler());
        netManager.addEventListener(DHT_EVENT_DELETE, new DHTDeleteHandler());

        globalKeyDirectory = new GlobalKeyDirectory();
    }

    //public static DHTManager getInstance() { return Preconditions.checkNotNull(globalDHTInstance.get()); }
    public static DHTManager getInstance() { return null; }

    public void clearContext() {
        globalKeyDirectory.clearContext();
        store.clear();
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class DHTPutValueHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            @SuppressWarnings("unchecked")
            final Pair<DHTKey,AbstractBufferedDHTObject> entry = (Pair<DHTKey,AbstractBufferedDHTObject>)e.getPayload();
            localPut(entry.getKey(), entry.getValue());
        }
    }

    private final class DHTPutSegmentsHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            @SuppressWarnings("unchecked")
            final Pair<DHTKey,AbstractBufferedDHTObject.Segment[]> request = (Pair<DHTKey,AbstractBufferedDHTObject.Segment[]>)e.getPayload();
            // Local put.
            final DHTKey key = globalKeyDirectory.get(request.getLeft().internalUID);
            final AbstractBufferedDHTObject value = __get(key);
            value.putSegments(request.getValue(), nodeID);
            logDHTAction(key, DHTAction.PUT_SEGMENT);
        }
    }

    private final class DHTGetValueRequestHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                final NetEvent event = (NetEvent) e;
                @SuppressWarnings("unchecked")
                final Pair<UUID,DHTKey> request = (Pair<UUID,DHTKey>) event.getPayload();
                final DHTKey key = globalKeyDirectory.get(request.getRight().internalUID);
                Preconditions.checkState(key != null);
                final AbstractBufferedDHTObject value = __get(key);
                //value.compress(); // TODO: Compression!
                final NetEvent e1 = new NetEvent(DHT_EVENT_GET_VALUE_RESPONSE);
                e1.setPayload(Pair.of(request.getKey(), value));
                netManager.dispatchEventAt(event.srcMachineID, e1);
                logDHTAction(key, DHTAction.GET_VALUE);
            });
        }
    }

    private final class DHTGetValueResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                @SuppressWarnings("unchecked")
                final Pair<UUID,AbstractBufferedDHTObject> response = (Pair<UUID,AbstractBufferedDHTObject>) e.getPayload();
                final AbstractBufferedDHTObject value = response.getValue();
                //value.decompress(); // TODO: Decompression!
                responseValueTable.put(response.getKey(), value);
                requestTable.remove(response.getKey()).countDown();
            });
        }
    }

    private final class DHTGetSegmentsRequestHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                final NetEvent event = (NetEvent) e;
                @SuppressWarnings("unchecked")
                final Triple<UUID,DHTKey,int[]> segmentsRequest = (Triple<UUID,DHTKey,int[]>) event.getPayload();
                final DHTKey key = globalKeyDirectory.get(segmentsRequest.getMiddle().internalUID);
                final AbstractBufferedDHTObject.Segment[] segments = __get(segmentsRequest.getMiddle()).getSegments(segmentsRequest.getRight(), nodeID);
                final NetEvent e1 = new NetEvent(DHT_EVENT_GET_SEGMENTS_RESPONSE);
                e1.setPayload(Pair.of(segmentsRequest.getLeft(), segments));
                netManager.dispatchEventAt(event.srcMachineID, e1);
                logDHTAction(key, DHTAction.GET_SEGMENT);
            });
        }
    }

    private final class DHTGetSegmentsResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                @SuppressWarnings("unchecked")
                final Pair<UUID,AbstractBufferedDHTObject.Segment[]> response = (Pair<UUID,AbstractBufferedDHTObject.Segment[]>) e.getPayload();
                responseSegmentsTable.put(response.getKey(), response.getValue());
                requestTable.remove(response.getKey()).countDown();
            });
        }
    }

    private final class DHTDeleteHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                final NetEvent event = (NetEvent)e;
                final DHTKey key = globalKeyDirectory.get(((DHTKey) event.getPayload()).internalUID);
                // Local delete.
                globalKeyDirectory.remove(key);
                if (__remove(key) == null)
                    throw new IllegalStateException();
                logDHTAction(key, DHTAction.DELETE_VALUE);
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public List<MachineDescriptor> getDHTNodes() { return Collections.unmodifiableList(infraManager.getMachines()); }

    public int getNumberOfDHTNodes() { return getDHTNodes().size(); }

    // ---------------------------------------------------

    /**
     * The put apply stores, according to a given key, a <code>Value</code> object in the distributed hash table.
     * The apply also accept a array of values, so-called value partitions. These partitions are distributed over
     * the at in the cluster. The key provides the complete metadata of all distributed partitions, the so-called
     * partition directory.
     * @param key The key that is associated with the value object.
     * @param vals A value object, or a array of value partitions.
     * @return The key containing the distribution metadata of all value partitions.
     */
    public DHTKey put(final DHTKey key, final AbstractBufferedDHTObject vals) { return put(key, new AbstractBufferedDHTObject[] {vals}, AbstractBufferedDHTObject.DEFAULT_SEGMENT_SIZE); }
    public DHTKey put(final DHTKey key, final AbstractBufferedDHTObject[] vals) { return put(key, vals, AbstractBufferedDHTObject.DEFAULT_SEGMENT_SIZE); }
    public DHTKey put(final DHTKey key, final AbstractBufferedDHTObject[] vals, int segmentSize) {

        if ((key.getPartitionDirectory() == null || key.getPartitionDirectory().size() == 0)
                && globalKeyDirectory.get(key.internalUID) == null) {

            // Select the machine where the value or the first value partition is stored.
            final MachineDescriptor primaryMachine = selectMachineForKey(key);
            // The beginning segment index of this partition.
            int basePartitionSegmentIndex = 0;
            // Create an partition descriptor of this value.
            final DHTKey.PartitionDescriptor ppd =
                    new DHTKey.PartitionDescriptor(
                            0,                                          // Partition index.
                            vals[0].getPartitionSize(),                 // Size of the partition.
                            0,                                          // Global byte offset.
                            basePartitionSegmentIndex,                  // Beginning segment index of this partition.
                            vals[0].getPartitionSize() / segmentSize,   // Number of segments this partition consists of.
                            segmentSize,                                // Size of a segment. (all segments have equal length).
                            primaryMachine                              // The machine where the partition is stored.
                    );
            // Add descriptor to the keys' partition directory.
            key.addPartitionDirectoryEntry(nodeID, ppd);

            // At the moment we does not allow local storage of multiple values...
            if (vals.length > 1 && key.distributionMode == DHTKey.DistributionMode.LOCAL)
                throw new IllegalStateException();

            // If we have more value partitions.
            if (vals.length > 1) {
                // Create a list of machines where the remaining value partitions are stored.
                final List<MachineDescriptor> secondaryMachines = new ArrayList<>(getDHTNodes());
                secondaryMachines.remove(primaryMachine);
                // Compute beginning segment index of the next value partition.
                basePartitionSegmentIndex += vals[0].getPartitionSize() / segmentSize;
                // Compute global offset of the value partition.
                int globalOffset = vals[0].getPartitionSize();
                // Iterate over the remaining value partitions and
                // create the corresponding partition descriptors.
                for (int i = 1; i < vals.length; ++i) {
                    final MachineDescriptor remoteMachine = secondaryMachines.get(i - 1);
                    final DHTKey.PartitionDescriptor pd =
                            new DHTKey.PartitionDescriptor(
                                    i,                              // Partition index.
                                    vals[i].getPartitionSize(),     // Size of the partition.
                                    globalOffset,                   // Global byte offset
                                    basePartitionSegmentIndex,      // Beginning segment index of this partition.
                                    vals[i].getPartitionSize() / segmentSize, // Number of segments this partition consists of.
                                    segmentSize,                    // Size of a segment. (all segments have equal length).
                                    remoteMachine                   // The machine where the partition is stored.
                            );

                    // Add descriptor to the keys' partition directory.
                    key.addPartitionDirectoryEntry(infraManager.getMachineIndex(remoteMachine), pd);
                    basePartitionSegmentIndex += pd.numberOfSegments;
                    globalOffset += vals[i].getPartitionSize();
                }
            }

            globalKeyDirectory.globalPut(key);
        }

        // Iterate over the partition directory and distribute
        // the value partitions to their assigned machines.
        final Iterator<DHTKey.PartitionDescriptor> it = key.getPartitionDirectory().values().iterator();
        for (int i = 0; i < key.getPartitionDirectory().size(); ++i) {
            final DHTKey.PartitionDescriptor pd = it.next();
            if (isLocal(pd.machine)) {
                localPut(key, vals[i]);
            } else {
                // Remote put.
                final NetEvent e = new NetEvent(DHT_EVENT_PUT_VALUE);
                e.setPayload(Pair.of(key, vals[i]));
                netManager.dispatchEventAt(pd.machine, e);
            }
        }

        return key;
    }

    private void localPut(final DHTKey key, final AbstractBufferedDHTObject val) {
        // Set the key for the value partition.
        val.setInternalUID(key.internalUID);
        val.setKey(key);
        // Allocate memory for the value.
        if (!val.isAllocated())
            val.allocateMemory(nodeID);
        __put(key, val);
        logDHTAction(key, DHTAction.PUT_VALUE);
    }

    /**
     * Put back updated value segments to their dhtManager storage.
     * @param key The key that is associated with the value object.
     * @param segment Updated segments of a value object.
     */
    public void put(final DHTKey key, final AbstractBufferedDHTObject.Segment segment) { put(key, new AbstractBufferedDHTObject.Segment[] { segment }); }
    public void put(final DHTKey key, final AbstractBufferedDHTObject.Segment[] segments) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(segments);
        // Group all segments according to their storage locations/dhtManager at.
        final Map<MachineDescriptor, List<AbstractBufferedDHTObject.Segment>> putRequests = new HashMap<>();
        for (final AbstractBufferedDHTObject.Segment segment : segments) {
            final MachineDescriptor md = key.getDHTNodeFromSegmentIndex(segment.segmentIndex);
            List<AbstractBufferedDHTObject.Segment> segmentsToPut = putRequests.get(md);
            if (segmentsToPut == null) {
                segmentsToPut = new ArrayList<>();
                putRequests.put(md, segmentsToPut);
            }
            segmentsToPut.add(segment);
        }
        // Iterate over the grouped segments and push them to their storage locations.
        // Internally we span multiple threads to parallelize the put requests.
        for (final Map.Entry<MachineDescriptor, List<AbstractBufferedDHTObject.Segment>> e : putRequests.entrySet()) {
            final AbstractBufferedDHTObject.Segment[] segs = new AbstractBufferedDHTObject.Segment[e.getValue().size()];
            e.getValue().toArray(segs);
            if (isLocal(e.getKey())) {
                // Local put.
                final AbstractBufferedDHTObject value = __get(key);
                value.putSegments(segs, nodeID);
                logDHTAction(key, DHTAction.PUT_SEGMENT);
            } else {
                // Remote put.
                executor.submit(() -> {
                    final NetEvent event = new NetEvent(DHT_EVENT_PUT_SEGMENTS);
                    event.setPayload(Pair.of(key, segs));
                    netManager.dispatchEventAt(e.getKey(), event);
                });
            }
        }
    }

    // ---------------------------------------------------

    /**
     * Get a <Code>Value</Code> from the dhtManager.
     * @param key The key that is associated with the value object.
     * @return The gathered <Code>Value</Code> partitions.
     */
    public AbstractBufferedDHTObject[] get(final DHTKey key) {
        Preconditions.checkNotNull(key);
        final int numberOfPartitions = key.getPartitionDirectory().size();
        final AbstractBufferedDHTObject[] values = new AbstractBufferedDHTObject[numberOfPartitions];
        final CountDownLatch operationCompleteLatch = new CountDownLatch(numberOfPartitions);
        // Iterate over keys' partition directory and request all partitions.
        // Internally we span multiple threads to parallelize the value requests.
        for (final Map.Entry<Integer,DHTKey.PartitionDescriptor> entry : key.getPartitionDirectory().entrySet()) {
            final DHTKey.PartitionDescriptor pd = entry.getValue();
            if (isLocal(pd.machine)) {
                values[pd.partitionIndex] = __get(key);
                logDHTAction(key, DHTAction.GET_VALUE);
                operationCompleteLatch.countDown();
            } else {
                executor.submit(() -> {
                    values[pd.partitionIndex] = getRemoteValueBlocking(pd.machine, key);
                    operationCompleteLatch.countDown();
                });
            }
        }
        try {
            // Wait until all value partitions are collected from remote at.
            operationCompleteLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return values;
    }

    private AbstractBufferedDHTObject getRemoteValueBlocking(final MachineDescriptor machine, final DHTKey key) {
        final CountDownLatch cdl = new CountDownLatch(1);
        final UUID requestID = UUID.randomUUID();
        requestTable.put(requestID, cdl);
        final NetEvent e = new NetEvent(DHT_EVENT_GET_VALUE_REQUEST);
        e.setPayload(Pair.of(requestID, key));
        netManager.dispatchEventAt(machine, e);
        try {
            if (RESPONSE_TIMEOUT > 0) {
                // block the caller thread until we get some response...
                // ...but with a specified timeout to avoid indefinitely blocking of caller.
                cdl.await(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
            } else
                cdl.await();
        } catch (InterruptedException ie) {
            throw new IllegalStateException(ie);
        }
        return responseValueTable.remove(requestID);
    }

    // The returned segment array order does correspond to order in segmentIndices.
    public AbstractBufferedDHTObject.Segment[] get(final DHTKey key, final int segmentIndex) { return get(key, new int[] { segmentIndex }); }
    public AbstractBufferedDHTObject.Segment[] get(final DHTKey key, final int[] segmentIndices) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(segmentIndices);
        final AbstractBufferedDHTObject.Segment[] segments = new AbstractBufferedDHTObject.Segment[segmentIndices.length];
        // Build all requests and group them according to the dhtManager at.
        final Map<MachineDescriptor, List<Integer>> requests = new HashMap<>();
        for (final int segmentIndex : segmentIndices) {
            final MachineDescriptor md = key.getDHTNodeFromSegmentIndex(segmentIndex);
            List<Integer> requestedSegmentIndices = requests.get(md);
            if (requestedSegmentIndices == null) {
                requestedSegmentIndices = new ArrayList<>();
                requests.put(md, requestedSegmentIndices);
            }
            requestedSegmentIndices.add(segmentIndex);
        }
        // Iterate over keys' partition directory and request all segments.
        // Internally we span multiple threads to parallelize the collecting process.
        final CountDownLatch operationCompleteLatch = new CountDownLatch(requests.size());
        for (final Map.Entry<MachineDescriptor, List<Integer>> e : requests.entrySet()) {
            if (isLocal(e.getKey())) {
                final AbstractBufferedDHTObject.Segment[] localSegments = __get(key).getSegments(Ints.toArray(e.getValue()), nodeID);
                for (final AbstractBufferedDHTObject.Segment localSegment : localSegments) {
                    final int index = ArrayUtils.indexOf(segmentIndices, localSegment.segmentIndex);
                    segments[index] = localSegment;
                }
                logDHTAction(key, DHTAction.GET_SEGMENT);
                operationCompleteLatch.countDown();
            } else {
                executor.submit(() -> {
                    final AbstractBufferedDHTObject.Segment[] remoteSegments = getRemoteSegmentsBlocking(e.getKey(), key, Ints.toArray(e.getValue()));
                    for (final AbstractBufferedDHTObject.Segment remoteSegment : remoteSegments) {
                        final int index = ArrayUtils.indexOf(segmentIndices, remoteSegment.segmentIndex);
                        segments[index] = remoteSegment;
                    }
                    operationCompleteLatch.countDown();
                });
            }
        }
        try {
            // Wait until all segments are collected from remote at.
            operationCompleteLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return segments;
    }

    private AbstractBufferedDHTObject.Segment[] getRemoteSegmentsBlocking(final MachineDescriptor machine, final DHTKey key, final int[] segmentIndices) {
        final CountDownLatch cdl = new CountDownLatch(1);
        final UUID requestID = UUID.randomUUID();
        requestTable.put(requestID, cdl);
        final NetEvent event = new NetEvent(DHT_EVENT_GET_SEGMENTS_REQUEST);
        event.setPayload(Triple.of(requestID, key, segmentIndices));
        netManager.dispatchEventAt(machine, event);
        try {
            if (RESPONSE_TIMEOUT > 0) {
                // block the caller thread until we get some response...
                // ...but with a specified timeout to avoid indefinitely blocking of caller.
                cdl.await(RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
            } else
                cdl.await();
        } catch (InterruptedException ie) {
            throw new IllegalStateException(ie);
        }
        return responseSegmentsTable.remove(requestID);
    }

    // ---------------------------------------------------

    /**
     * Delete a <code>Value</code> object (and associated partitions) in the dhtManager.
     * @param key The key that is associated with the value object.
     */
    public void delete(final DHTKey key) {
        //final Key k = keyDirectory.get(key.internalUID);
        final DHTKey k = globalKeyDirectory.get(key.internalUID);
        // At the moment we need a key with all the distribution metadata.
        Preconditions.checkState(k != null);
        // Iterate over keys' partition directory and either push
        // delete messages to remote partitions or delete locally.
        for (final DHTKey.PartitionDescriptor pd : k.getPartitionDirectory().values())
            if (isLocal(pd.machine)) {
                // Local delete.
                if (/*keyDirectory.remove(key.internalUID) == null ||*/ __remove(key) == null)
                    throw new IllegalStateException();
                logDHTAction(key, DHTAction.DELETE_VALUE);
            } else {
                // Remote delete.
                final NetEvent e = new NetEvent(DHT_EVENT_DELETE);
                e.setPayload(key);
                netManager.dispatchEventAt(pd.machine, e);
            }

        globalKeyDirectory.globalRemove(key);
    }

    // ---------------------------------------------------

    public Set<DHTKey> getKey(final String name) { return globalKeyDirectory.get(Preconditions.checkNotNull(name)); }

    public DHTKey getKey(final UUID internalID) { return globalKeyDirectory.get(Preconditions.checkNotNull(internalID)); }

    // ---------------------------------------------------

    public AbstractBufferedDHTObject __get(final DHTKey key) {
        final AbstractBufferedDHTObject value = store.get(key);
        synchronized (value.lock) {
            return value;
        }
    }

    public AbstractBufferedDHTObject __put(final DHTKey key, final AbstractBufferedDHTObject value) {
        synchronized (value.lock) {
            return store.put(key, value);
        }
    }

    public AbstractBufferedDHTObject __remove(final DHTKey key) {
        final AbstractBufferedDHTObject value = store.remove(key);
        synchronized (value.lock) {
            return value;
        }
    }

    // ---------------------------------------------------

    /*public void lput(final DHTKey k, final DHTObject v) { lstore.put(Preconditions.checkNotNull(k), Preconditions.checkNotNull(v)); }

    public DHTObject lget(final DHTKey k) { return Preconditions.checkNotNull(lstore.get(k)); }

    public void ldelete(final DHTKey k) { lstore.remove(k); }*/

    // ---------------------------------------------------

    public UUID createLocalUID() {
        int id; UUID uid;
        do {
            uid = UUID.randomUUID();
            id = (uid.hashCode() & Integer.MAX_VALUE) % infraManager.getMachines().size();
        } while (id != infraManager.getNodeID());
        return uid;
    }

    public DHTKey createLocalKey(final String name) {
        final UUID localUID = createLocalUID();
        final DHTKey key = DHTKey.newKey(localUID, name, DHTKey.DistributionMode.DISTRIBUTED);
        return key;
    }

    // ---------------------------------------------------
    // Private Constants.
    // ---------------------------------------------------

    private enum DHTAction {

        PUT_VALUE,

        PUT_SEGMENT,

        GET_VALUE,

        GET_SEGMENT,

        DELETE_VALUE;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private MachineDescriptor selectMachineForKey(final DHTKey key) {
        if (key.distributionMode == DHTKey.DistributionMode.DISTRIBUTED)
            return selectMachineForKey(key.internalUID);
        else
            return netManager.getMachineDescriptor();
    }

    private MachineDescriptor selectMachineForKey(final UUID internalUID) {
        Preconditions.checkNotNull(internalUID);
        // Compute (simple hash partitioning) the partitionIndex
        // of the machine where to store the entry.
        final int machineIndex = (internalUID.hashCode() & Integer.MAX_VALUE) % infraManager.getMachines().size();
        return infraManager.getMachines().get(machineIndex);
    }

    private boolean isLocal(final MachineDescriptor machine) {
        return infraManager.getMachine().machineID.equals(Preconditions.checkNotNull(machine).machineID);
    }

    private void logDHTAction(final DHTKey key, final DHTAction action) {
        LOG.debug(action + " ON " + infraManager.getMachine() + " => KEY = "
                + key.internalUID + " | " + "PARTITION = " + key.getPartitionDescriptor(nodeID).partitionIndex);
    }
}
