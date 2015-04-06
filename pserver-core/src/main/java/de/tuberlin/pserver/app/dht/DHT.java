package de.tuberlin.pserver.app.dht;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.experimental.memory.Buffer;
import de.tuberlin.pserver.utils.Compressor;
import de.tuberlin.pserver.utils.nbhm.NonBlockingHashMap;
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

public final class DHT extends EventDispatcher {

    // ---------------------------------------------------
    // DHT Events.
    // ---------------------------------------------------

    public static final String DHT_EVENT_PUT_VALUE                  = "DPEV";

    public static final String DHT_EVENT_PUT_SEGMENTS               = "DPES";

    public static final String DHT_EVENT_GET_VALUE_REQUEST          = "DGERQV";

    public static final String DHT_EVENT_GET_VALUE_RESPONSE         = "DGERPV";

    public static final String DHT_EVENT_GET_SEGMENTS_REQUEST       = "DGERQR";

    public static final String DHT_EVENT_GET_SEGMENTS_RESPONSE      = "DGERPR";

    public static final String DHT_EVENT_DELETE                     = "DDE";

    public static final String DHT_EVENT_ADD_KEY_TO_DIRECTORY       = "DEAKTD";

    public static final String DHT_EVENT_REMOVE_KEY_FROM_DIRECTORY  = "DERKFD";

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class GlobalKeyDirectory {

        // ---------------------------------------------------

        private final class DHTAddKeyToDirectoryHandler implements IEventHandler {
            @Override
            public void handleEvent(final Event e) {
                put((Key)e.getPayload());
            }
        }

        private final class DHTRemoveKeyToDirectoryHandler implements IEventHandler {
            @Override
            public void handleEvent(final Event e) {
                remove((Key)e.getPayload());
            }
        }

        // ---------------------------------------------------

        private final Map<UUID, Key> uidKeyDirectory;

        private final Map<String,Map<UUID,Key>> keyDirectory;

        // ---------------------------------------------------

        public GlobalKeyDirectory() {
            this.uidKeyDirectory = new NonBlockingHashMap<>();
            this.keyDirectory = new NonBlockingHashMap<>();
            netManager.addEventListener(DHT_EVENT_ADD_KEY_TO_DIRECTORY, new DHTAddKeyToDirectoryHandler());
            netManager.addEventListener(DHT_EVENT_REMOVE_KEY_FROM_DIRECTORY, new DHTRemoveKeyToDirectoryHandler());
        }

        // ---------------------------------------------------

        public void put(final Key key) {
            Map<UUID,Key> keys = keyDirectory.get(Preconditions.checkNotNull(key.name));
            if (keys == null) {
                keys = new NonBlockingHashMap<>();
                keyDirectory.put(key.name, keys);
            }
            keys.put(key.internalUID, key);
            uidKeyDirectory.put(key.internalUID, key);
        }

        public void globalPut(final Key key) {
            put(key);
            final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_EVENT_ADD_KEY_TO_DIRECTORY);
            event.setPayload(key);
            netManager.broadcastEvent(event);
        }

        public Set<Key> get(final String name) { return new HashSet<>(keyDirectory.get(name).values()); }

        public Key get(final UUID uid) { return uidKeyDirectory.get(uid); }

        public void remove(final Key key) {
            final Map<UUID,Key> keys = keyDirectory.get(Preconditions.checkNotNull(key.name));
            Preconditions.checkState(keys != null);
            final Key k = keys.get(key.internalUID);
            Preconditions.checkState(k != null);
            keys.remove(k);
        }

        public void globalRemove(final Key key) {
            remove(key);
            final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_EVENT_REMOVE_KEY_FROM_DIRECTORY);
            event.setPayload(key);
            netManager.broadcastEvent(event);
        }
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private enum DHTAction {

        PUT_VALUE,

        PUT_SEGMENT,

        GET_VALUE,

        GET_SEGMENT,

        DELETE_VALUE,
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DHT.class);

    private long RESPONSE_TIMEOUT = 55000; // in ms

    // ---------------------------------------------------

    private static final Object globalDHTMutex = new Object();

    private static DHT globalDHTInstance = null;

    // ---------------------------------------------------

    private final IConfig config;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final Compressor.CompressionType compressionType;

    private final Map<Key,BufferValue> store;

    // ---------------------------------------

    private int instanceID;

    private final GlobalKeyDirectory globalKeyDirectory;

    // ---------------------------------------

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final Map<UUID,CountDownLatch> requestTable = new NonBlockingHashMap<>();

    private final Map<UUID,BufferValue> responseValueTable = new NonBlockingHashMap<>();

    private final Map<UUID,BufferValue.Segment[]> responseSegmentsTable = new NonBlockingHashMap<>();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DHT(final IConfig config,
               final InfrastructureManager infraManager,
               final NetManager netManager) {

        super(true, "DHT-THREAD");

        synchronized (globalDHTMutex) {
            if (globalDHTInstance == null)
                globalDHTInstance = this;
            else {
                throw new IllegalStateException();
            }
        }

        this.config         = Preconditions.checkNotNull(config);
        this.infraManager   = Preconditions.checkNotNull(infraManager);
        this.netManager     = Preconditions.checkNotNull(netManager);
        this.instanceID     = infraManager.getInstanceID();
        this.store          = new NonBlockingHashMap<>();

        this.compressionType = Compressor.CompressionType.valueOf(
                this.config.getString("dht.compression.compressionType")
        );

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

    public static DHT getInstance() { return globalDHTInstance; }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class DHTPutValueHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            @SuppressWarnings("unchecked")
            final Pair<Key,BufferValue> entry = (Pair<Key,BufferValue>)e.getPayload();
            localPut(entry.getKey(), entry.getValue());
        }
    }

    private final class DHTPutSegmentsHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            @SuppressWarnings("unchecked")
            final Pair<Key,BufferValue.Segment[]> request = (Pair<Key,BufferValue.Segment[]>)e.getPayload();
            // Local put.
            final Key key = globalKeyDirectory.get(request.getLeft().internalUID);
            final BufferValue value = store.get(key);
            value.putSegments(request.getValue(), instanceID);
            logDHTAction(key, DHTAction.PUT_SEGMENT);
        }
    }

    private final class DHTGetValueRequestHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
                @SuppressWarnings("unchecked")
                final Pair<UUID,Key> request = (Pair<UUID,Key>) event.getPayload();
                final Key key = globalKeyDirectory.get(request.getRight().internalUID);
                Preconditions.checkState(key != null);
                final BufferValue value = store.get(key);
                //value.compress(); // TODO: Compression!
                final NetEvents.NetEvent e1 = new NetEvents.NetEvent(DHT_EVENT_GET_VALUE_RESPONSE);
                e1.setPayload(Pair.of(request.getKey(), value));
                netManager.sendEvent(event.srcMachineID, e1);
                logDHTAction(key, DHTAction.GET_VALUE);
            });
        }
    }

    private final class DHTGetValueResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                @SuppressWarnings("unchecked")
                final Pair<UUID,BufferValue> response = (Pair<UUID,BufferValue>) e.getPayload();
                final BufferValue value = response.getValue();
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
                final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
                @SuppressWarnings("unchecked")
                final Triple<UUID,Key,int[]> segmentsRequest = (Triple<UUID,Key,int[]>) event.getPayload();
                final Key key = globalKeyDirectory.get(segmentsRequest.getMiddle().internalUID);
                final BufferValue.Segment[] segments = store.get(segmentsRequest.getMiddle()).getSegments(segmentsRequest.getRight(), instanceID);
                final NetEvents.NetEvent e1 = new NetEvents.NetEvent(DHT_EVENT_GET_SEGMENTS_RESPONSE);
                e1.setPayload(Pair.of(segmentsRequest.getLeft(), segments));
                netManager.sendEvent(event.srcMachineID, e1);
                logDHTAction(key, DHTAction.GET_SEGMENT);
            });
        }
    }

    private final class DHTGetSegmentsResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                @SuppressWarnings("unchecked")
                final Pair<UUID,BufferValue.Segment[]> response = (Pair<UUID,BufferValue.Segment[]>) e.getPayload();
                responseSegmentsTable.put(response.getKey(), response.getValue());
                requestTable.remove(response.getKey()).countDown();
            });
        }
    }

    private final class DHTDeleteHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(() -> {
                final NetEvents.NetEvent event = (NetEvents.NetEvent)e;
                final Key key = globalKeyDirectory.get(((Key) event.getPayload()).internalUID);
                // Local delete.
                globalKeyDirectory.remove(key);
                if (store.remove(key) == null)
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
     * The put operation stores, according to a given key, a <code>Value</code> object in the distributed hash table.
     * The operation also accept a array of values, so-called value partitions. These partitions are distributed over
     * the nodes in the cluster. The key provides the complete metadata of all distributed partitions, the so-called
     * partition directory.
     * @param key The key that is associated with the value object.
     * @param vals A value object, or a array of value partitions.
     * @return The key containing the distribution metadata of all value partitions.
     */
    public Key put(final Key key, final BufferValue vals) { return put(key, new BufferValue[] {vals}, BufferValue.DEFAULT_SEGMENT_SIZE); }
    public Key put(final Key key, final BufferValue[] vals) { return put(key, vals, BufferValue.DEFAULT_SEGMENT_SIZE); }
    public Key put(final Key key, final BufferValue[] vals, int segmentSize) {

        if ((key.getPartitionDirectory() == null || key.getPartitionDirectory().size() == 0)
                && globalKeyDirectory.get(key.internalUID) == null) {

            // Select the machine where the value or the first value partition is stored.
            final MachineDescriptor primaryMachine = selectMachineForKey(key);
            // The beginning segment index of this partition.
            int basePartitionSegmentIndex = 0;
            // Create an partition descriptor of this value.
            final Key.PartitionDescriptor ppd =
                    new Key.PartitionDescriptor(
                            0,                                          // Partition index.
                            vals[0].getPartitionSize(),                 // Size of the partition.
                            0,                                          // Global byte offset.
                            basePartitionSegmentIndex,                  // Beginning segment index of this partition.
                            vals[0].getPartitionSize() / segmentSize,   // Number of segments this partition consists of.
                            segmentSize,                                // Size of a segment. (all segments have equal size).
                            primaryMachine                              // The machine where the partition is stored.
                    );
            // Add descriptor to the keys' partition directory.
            key.addPartitionDirectoryEntry(instanceID, ppd);

            // At the moment we does not allow local storage of multiple values...
            if (vals.length > 1 && key.distributionMode == Key.DistributionMode.LOCAL)
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
                    final Key.PartitionDescriptor pd =
                            new Key.PartitionDescriptor(
                                    i,                              // Partition index.
                                    vals[i].getPartitionSize(),     // Size of the partition.
                                    globalOffset,                   // Global byte offset
                                    basePartitionSegmentIndex,      // Beginning segment index of this partition.
                                    vals[i].getPartitionSize() / segmentSize, // Number of segments this partition consists of.
                                    segmentSize,                    // Size of a segment. (all segments have equal size).
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
        final Iterator<Key.PartitionDescriptor> it = key.getPartitionDirectory().values().iterator();
        for (int i = 0; i < key.getPartitionDirectory().size(); ++i) {
            final Key.PartitionDescriptor pd = it.next();
            if (isLocal(pd.machine)) {
                localPut(key, vals[i]);
            } else {
                // Remote put.
                final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_EVENT_PUT_VALUE);
                e.setPayload(Pair.of(key, vals[i]));
                netManager.sendEvent(pd.machine, e);
            }
        }

        return key;
    }

    private void localPut(final Key key, final BufferValue val) {
        // Set the key for the value partition.
        val.setInternalUID(key.internalUID);
        val.setKey(key);
        // Allocate memory for the value.
        if (!val.isAllocated())
            val.allocateMemory(instanceID);
        store.put(key, val);
        logDHTAction(key, DHTAction.PUT_VALUE);
    }

    /**
     * Put back updated value segments to their dht storage.
     * @param key The key that is associated with the value object.
     * @param segment Updated segments of a value object.
     */
    public void put(final Key key, final BufferValue.Segment segment) { put(key, new BufferValue.Segment[] { segment }); }
    public void put(final Key key, final BufferValue.Segment[] segments) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(segments);
        // Group all segments according to their storage locations/dht nodes.
        final Map<MachineDescriptor, List<BufferValue.Segment>> putRequests = new HashMap<>();
        for (final BufferValue.Segment segment : segments) {
            final MachineDescriptor md = key.getDHTNodeFromSegmentIndex(segment.segmentIndex);
            List<BufferValue.Segment> segmentsToPut = putRequests.get(md);
            if (segmentsToPut == null) {
                segmentsToPut = new ArrayList<>();
                putRequests.put(md, segmentsToPut);
            }
            segmentsToPut.add(segment);
        }
        // Iterate over the grouped segments and push them to their storage locations.
        // Internally we span multiple threads to parallelize the put requests.
        for (final Map.Entry<MachineDescriptor, List<BufferValue.Segment>> e : putRequests.entrySet()) {
            final BufferValue.Segment[] segs = new BufferValue.Segment[e.getValue().size()];
            e.getValue().toArray(segs);
            if (isLocal(e.getKey())) {
                // Local put.
                final BufferValue value = store.get(key);
                value.putSegments(segs, instanceID);
                logDHTAction(key, DHTAction.PUT_SEGMENT);
            } else {
                // Remote put.
                executor.submit(() -> {
                    final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_EVENT_PUT_SEGMENTS);
                    event.setPayload(Pair.of(key, segs));
                    netManager.sendEvent(e.getKey(), event);
                });
            }
        }
    }

    // ---------------------------------------------------

    /**
     * Get a <Code>Value</Code> from the dht.
     * @param key The key that is associated with the value object.
     * @return The gathered <Code>Value</Code> partitions.
     */
    public BufferValue[] get(final Key key) {
        Preconditions.checkNotNull(key);
        final int numberOfPartitions = key.getPartitionDirectory().size();
        final BufferValue[] values = new BufferValue[numberOfPartitions];
        final CountDownLatch operationCompleteLatch = new CountDownLatch(numberOfPartitions);
        // Iterate over keys' partition directory and request all partitions.
        // Internally we span multiple threads to parallelize the value requests.
        for (final Map.Entry<Integer,Key.PartitionDescriptor> entry : key.getPartitionDirectory().entrySet()) {
            final Key.PartitionDescriptor pd = entry.getValue();
            if (isLocal(pd.machine)) {
                values[pd.partitionIndex] = store.get(key);
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
            // Wait until all value partitions are collected from remote nodes.
            operationCompleteLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return values;
    }

    private BufferValue getRemoteValueBlocking(final MachineDescriptor machine, final Key key) {
        final CountDownLatch cdl = new CountDownLatch(1);
        final UUID requestID = UUID.randomUUID();
        requestTable.put(requestID, cdl);
        final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_EVENT_GET_VALUE_REQUEST);
        e.setPayload(Pair.of(requestID, key));
        netManager.sendEvent(machine, e);
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
    public BufferValue.Segment[] get(final Key key, final int segmentIndex) { return get(key, new int[] { segmentIndex }); }
    public BufferValue.Segment[] get(final Key key, final int[] segmentIndices) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(segmentIndices);
        final BufferValue.Segment[] segments = new BufferValue.Segment[segmentIndices.length];
        // Build all requests and group them according to the dht nodes.
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
                final BufferValue.Segment[] localSegments = store.get(key).getSegments(Ints.toArray(e.getValue()), instanceID);
                for (final BufferValue.Segment localSegment : localSegments) {
                    final int index = ArrayUtils.indexOf(segmentIndices, localSegment.segmentIndex);
                    segments[index] = localSegment;
                }
                logDHTAction(key, DHTAction.GET_SEGMENT);
                operationCompleteLatch.countDown();
            } else {
                executor.submit(() -> {
                    final BufferValue.Segment[] remoteSegments = getRemoteSegmentsBlocking(e.getKey(), key, Ints.toArray(e.getValue()));
                    for (final BufferValue.Segment remoteSegment : remoteSegments) {
                        final int index = ArrayUtils.indexOf(segmentIndices, remoteSegment.segmentIndex);
                        segments[index] = remoteSegment;
                    }
                    operationCompleteLatch.countDown();
                });
            }
        }
        try {
            // Wait until all segments are collected from remote nodes.
            operationCompleteLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return segments;
    }

    private BufferValue.Segment[] getRemoteSegmentsBlocking(final MachineDescriptor machine, final Key key, final int[] segmentIndices) {
        final CountDownLatch cdl = new CountDownLatch(1);
        final UUID requestID = UUID.randomUUID();
        requestTable.put(requestID, cdl);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_EVENT_GET_SEGMENTS_REQUEST);
        event.setPayload(Triple.of(requestID, key, segmentIndices));
        netManager.sendEvent(machine, event);
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
     * Delete a <code>Value</code> object (and associated partitions) in the dht.
     * @param key The key that is associated with the value object.
     */
    public void delete(final Key key) {
        //final Key k = keyDirectory.get(key.internalUID);
        final Key k = globalKeyDirectory.get(key.internalUID);
        // At the moment we need a key with all the distribution metadata.
        Preconditions.checkState(k != null);
        // Iterate over keys' partition directory and either push
        // delete messages to remote partitions or delete locally.
        for (final Key.PartitionDescriptor pd : k.getPartitionDirectory().values())
            if (isLocal(pd.machine)) {
                // Local delete.
                if (/*keyDirectory.remove(key.internalUID) == null ||*/ store.remove(key) == null)
                    throw new IllegalStateException();
                logDHTAction(key, DHTAction.DELETE_VALUE);
            } else {
                // Remote delete.
                final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_EVENT_DELETE);
                e.setPayload(key);
                netManager.sendEvent(pd.machine, e);
            }

        globalKeyDirectory.globalRemove(key);
    }

    // ---------------------------------------------------

    public Set<Key> getKey(final String name) { return globalKeyDirectory.get(Preconditions.checkNotNull(name)); }

    public Key getKey(final UUID internalID) { return globalKeyDirectory.get(Preconditions.checkNotNull(internalID)); }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private MachineDescriptor selectMachineForKey(final Key key) {
        if (key.distributionMode == Key.DistributionMode.DISTRIBUTED)
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

    private void logDHTAction(final Key key, final DHTAction action) {
        LOG.info(action + " ON " + infraManager.getMachine() + " => KEY = "
                + key.internalUID + " | " + "PARTITION = " + key.getPartitionDescriptor(instanceID).partitionIndex);
    }
}
