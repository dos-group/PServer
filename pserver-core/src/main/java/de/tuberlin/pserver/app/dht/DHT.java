package de.tuberlin.pserver.app.dht;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.utils.nbhm.NonBlockingHashMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class DHT extends EventDispatcher {

    // ---------------------------------------------------
    // dht Events.
    // ---------------------------------------------------

    public static final String DHT_PUT_EVENT_VALUE              = "DPEV";
    public static final String DHT_PUT_EVENT_SEGMENTS           = "DPES";
    public static final String DHT_GET_EVENT_VALUE_REQUEST      = "DGERQV";
    public static final String DHT_GET_EVENT_VALUE_RESPONSE     = "DGERPV";
    public static final String DHT_GET_EVENT_SEGMENTS_REQUEST   = "DGERQR";
    public static final String DHT_GET_EVENT_SEGMENTS_RESPONSE  = "DGERPR";
    public static final String DHT_DEL_EVENT                    = "DDE";
    public static final String DHT_GET_EVENT_KEY_REQUEST        = "DGERQK";
    public static final String DHT_GET_EVENT_KEY_RESPONSE       = "DGERPK";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DHT.class);

    private long RESPONSE_TIMEOUT = 55000; // in ms

    private static DHT instance = new DHT();

    private final ConcurrentMap<UUID, Key> keyDirectory;

    private final ConcurrentMap<Key,Value> store;

    private InfrastructureManager infraManager;

    private NetManager netManager;

    // ---------------------------------------

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final ConcurrentMap<UUID,CountDownLatch> requestTable = new NonBlockingHashMap<>();

    private final ConcurrentMap<UUID,Value> responseValueTable = new NonBlockingHashMap<>();

    private final ConcurrentMap<UUID,Value.Segment[]> responseSegmentsTable = new NonBlockingHashMap<>();

    private final ConcurrentMap<UUID,Key> responseKeyTable = new NonBlockingHashMap<>();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private DHT() {
        super(true, "KV-STORE-THREAD");
        this.keyDirectory = new NonBlockingHashMap<>();
        this.store = new NonBlockingHashMap<>();
    }

    public static DHT getInstance () { return DHT.instance; }

    // ---------------------------------------------------
    // Event Handler.
    // ---------------------------------------------------

    private final class DHTPutValueHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            @SuppressWarnings("unchecked")
            final Pair<Key,Value> entry = (Pair<Key,Value>)e.getPayload();
            localPut(entry.getKey(), entry.getValue());
        }
    }

    private final class DHTPutSegmentsHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            @SuppressWarnings("unchecked")
            final Pair<Key,Value.Segment[]> entry = (Pair<Key,Value.Segment[]>)e.getPayload();
            // Local put.
            final Value value = store.get(entry.getKey());
            value.putSegments(entry.getValue());
            logDHTAction(entry.getKey(), DHTAction.PUT_SEGMENT);
        }
    }

    private final class DHTGetValueRequestHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
               @Override
               public void run() {
                   final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
                   @SuppressWarnings("unchecked")
                   final Pair<UUID,Key> request = (Pair<UUID,Key>) event.getPayload();
                   final Key key = keyDirectory.get(request.getRight().internalUID);
                   Preconditions.checkState(key != null);
                   final Value value = store.get(key);
                   final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_GET_EVENT_VALUE_RESPONSE);
                   e.setPayload(Pair.of(request.getKey(), value));
                   netManager.sendEvent(event.srcMachineID, e);
                   logDHTAction(key, DHTAction.GET_VALUE);
               }
           });
        }
    }

    private final class DHTGetValueResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    @SuppressWarnings("unchecked")
                    final Pair<UUID,Value> response = (Pair<UUID,Value>) e.getPayload();
                    responseValueTable.put(response.getKey(), response.getValue());
                    requestTable.remove(response.getKey()).countDown();
                }
            });
        }
    }

    private final class DHTGetSegmentsRequestHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
                    @SuppressWarnings("unchecked")
                    final Triple<UUID,Key,int[]> segmentsRequest = (Triple<UUID,Key,int[]>) event.getPayload();
                    final Key key = keyDirectory.get(segmentsRequest.getMiddle().internalUID);
                    final Value.Segment[] segments = store.get(segmentsRequest.getMiddle()).getSegments(segmentsRequest.getRight());
                    final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_GET_EVENT_SEGMENTS_RESPONSE);
                    e.setPayload(Pair.of(segmentsRequest.getLeft(), segments));
                    netManager.sendEvent(event.srcMachineID, e);
                    logDHTAction(key, DHTAction.GET_SEGMENT);
                }
            });
        }
    }

    private final class DHTGetSegmentsResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    @SuppressWarnings("unchecked")
                    final Pair<UUID,Value.Segment[]> response = (Pair<UUID,Value.Segment[]>) e.getPayload();
                    responseSegmentsTable.put(response.getKey(), response.getValue());
                    requestTable.remove(response.getKey()).countDown();
                }
            });
        }
    }

    private final class DHTDeleteHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final NetEvents.NetEvent event = (NetEvents.NetEvent)e;
                    Key key = keyDirectory.get(((Key) event.getPayload()).internalUID);
                    // Local delete.
                    if (keyDirectory.remove(key.internalUID) == null || store.remove(key) == null)
                        throw new IllegalStateException();
                    logDHTAction(key, DHTAction.DELETE_VALUE);
                }
            });
        }
    }

    private final class DHTGetKeyRequestHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final NetEvents.NetEvent requestEvent = (NetEvents.NetEvent)e;
                    @SuppressWarnings("unchecked")
                    final Pair<UUID,UUID> keyRequest = (Pair<UUID,UUID>) requestEvent.getPayload();
                    Key key = null;
                    while(key == null) {
                        key = keyDirectory.get(keyRequest.getRight());
                        if (key == null) {
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }
                    final NetEvents.NetEvent responseEvent = new NetEvents.NetEvent(DHT_GET_EVENT_KEY_RESPONSE);
                    responseEvent.setPayload(Pair.of(keyRequest.getLeft(), key));
                    netManager.sendEvent(requestEvent.srcMachineID, responseEvent);
                }
            });
        }
    }

    private final class DHTGetKeyResponseHandler implements IEventHandler {
        @Override
        public void handleEvent(final Event e) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    @SuppressWarnings("unchecked")
                    final Pair<UUID,Key> response = (Pair<UUID,Key>) e.getPayload();
                    responseKeyTable.put(response.getKey(), response.getValue());
                    requestTable.remove(response.getKey()).countDown();
                }
            });
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public List<MachineDescriptor> getDHTNodes() { return Collections.unmodifiableList(infraManager.getMachines()); }

    public int getNumberOfDHTNodes() { return getDHTNodes().size(); }

    public void initialize(final InfrastructureManager infraManager, final NetManager netManager) {
        this.infraManager = Preconditions.checkNotNull(infraManager);
        this.netManager = Preconditions.checkNotNull(netManager);
        // Register dht events.
        netManager.addEventListener(DHT_PUT_EVENT_VALUE, new DHTPutValueHandler());
        netManager.addEventListener(DHT_PUT_EVENT_SEGMENTS, new DHTPutSegmentsHandler());
        netManager.addEventListener(DHT_GET_EVENT_VALUE_REQUEST, new DHTGetValueRequestHandler());
        netManager.addEventListener(DHT_GET_EVENT_VALUE_RESPONSE, new DHTGetValueResponseHandler());
        netManager.addEventListener(DHT_GET_EVENT_SEGMENTS_REQUEST, new DHTGetSegmentsRequestHandler());
        netManager.addEventListener(DHT_GET_EVENT_SEGMENTS_RESPONSE, new DHTGetSegmentsResponseHandler());
        netManager.addEventListener(DHT_DEL_EVENT, new DHTDeleteHandler());
        netManager.addEventListener(DHT_GET_EVENT_KEY_REQUEST, new DHTGetKeyRequestHandler());
        netManager.addEventListener(DHT_GET_EVENT_KEY_RESPONSE, new DHTGetKeyResponseHandler());
    }

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
    public Key put(final Key key, final Value vals) { return put(key, new Value[] {vals}, Key.DEFAULT_SEGMENT_SIZE); }
    public Key put(final Key key, final Value[] vals) { return put(key, vals, Key.DEFAULT_SEGMENT_SIZE); }
    public Key put(final Key key, final Value[] vals, int segmentSize) {
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
        key.addPartitionDirectoryEntry(0, ppd);

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
                final Key.PartitionDescriptor pd =
                        new Key.PartitionDescriptor(
                                i,                              // Partition index.
                                vals[i].getPartitionSize(),     // Size of the partition.
                                globalOffset,                   // Global byte offset
                                basePartitionSegmentIndex,      // Beginning segment index of this partition.
                                vals[i].getPartitionSize() / segmentSize, // Number of segments this partition consists of.
                                segmentSize,                    // Size of a segment. (all segments have equal size).
                                secondaryMachines.get(i - 1)    // The machine where the partition is stored.
                        );

                // Add descriptor to the keys' partition directory.
                key.addPartitionDirectoryEntry(i, pd);
                basePartitionSegmentIndex += pd.numberOfSegments;
                globalOffset += vals[i].getPartitionSize();
            }
        }
        // Iterate over the partition directory and distribute
        // the value partitions to their assigned machines.
        for (int i = 0; i < key.getPartitionDirectory().size(); ++i) {
            final Key.PartitionDescriptor pd =  key.getPartitionDirectory().get(i);
            if (isLocal(pd.machine)) {
                key.setPartitionDescriptor(pd);
                localPut(key, vals[i]);
            } else {
                // Remote put.
                final Key k = Key.copyKey(key, pd);
                final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_PUT_EVENT_VALUE);
                e.setPayload(Pair.of(k, vals[i]));
                netManager.sendEvent(pd.machine, e);
            }
        }
        return key;
    }

    private void localPut(final Key key, final Value val) {
        // Set the key for the value partition.
        val.setInternalUID(key.internalUID);
        val.setKey(key);
        // Allocate memory for the value.
        if (!val.isAllocated())
            val.allocateMemory();
        store.put(key, val);
        keyDirectory.put(key.internalUID, key);
        logDHTAction(key, DHTAction.PUT_VALUE);
    }

    /**
     * Put back updated value segments to their dht storage.
     * @param key The key that is associated with the value object.
     * @param segment Updated segments of a value object.
     */
    public void put(final Key key, final Value.Segment segment) { put(key, new Value.Segment[] { segment }); }
    public void put(final Key key, final Value.Segment[] segments) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(segments);
        // Group all segments according to their storage locations/dht nodes.
        final Map<MachineDescriptor, List<Value.Segment>> putRequests = new HashMap<>();
        for (final Value.Segment segment : segments) {
            final MachineDescriptor md = key.getDHTNodeFromSegmentIndex(segment.segmentIndex);
            List<Value.Segment> segmentsToPut = putRequests.get(md);
            if (segmentsToPut == null) {
                segmentsToPut = new ArrayList<>();
                putRequests.put(md, segmentsToPut);
            }
            segmentsToPut.add(segment);
        }
        // Iterate over the grouped segments and push them to their storage locations.
        // Internally we span multiple threads to parallelize the put requests.
        for (final Map.Entry<MachineDescriptor, List<Value.Segment>> e : putRequests.entrySet()) {
            final Value.Segment[] segs = new Value.Segment[e.getValue().size()];
            e.getValue().toArray(segs);
            if (isLocal(e.getKey())) {
                // Local put.
                final Value value = store.get(key);
                value.putSegments(segs);
                logDHTAction(key, DHTAction.PUT_SEGMENT);
            } else {
                // Remote put.
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_PUT_EVENT_SEGMENTS);
                        event.setPayload(Pair.of(key, segs));
                        netManager.sendEvent(e.getKey(), event);
                    }
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
    public Value[] get(final Key key) {
        Preconditions.checkNotNull(key);
        final int numberOfPartitions = key.getPartitionDirectory().size();
        final Value[] values = new Value[numberOfPartitions];
        final CountDownLatch operationCompleteLatch = new CountDownLatch(numberOfPartitions);
        // Iterate over keys' partition directory and request all partitions.
        // Internally we span multiple threads to parallelize the value requests.
        for (final Map.Entry<Integer,Key.PartitionDescriptor> entry : key.getPartitionDirectory().entrySet()) {
            final Key.PartitionDescriptor pd = entry.getValue();
            final int partitionIndex = entry.getKey();
            if (isLocal(pd.machine)) {
                values[partitionIndex] = store.get(key);
                logDHTAction(key, DHTAction.GET_VALUE);
                operationCompleteLatch.countDown();
            } else {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        values[partitionIndex] = getRemoteValueBlocking(pd.machine, key);
                        operationCompleteLatch.countDown();
                    }
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

    private Value getRemoteValueBlocking(final MachineDescriptor machine, final Key key) {
        final CountDownLatch cdl = new CountDownLatch(1);
        final UUID requestID = UUID.randomUUID();
        requestTable.put(requestID, cdl);
        final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_GET_EVENT_VALUE_REQUEST);
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
    public Value.Segment[] get(final Key key, final int segmentIndex) { return get(key, new int[] { segmentIndex }); }
    public Value.Segment[] get(final Key key, final int[] segmentIndices) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(segmentIndices);
        final Value.Segment[] segments = new Value.Segment[segmentIndices.length];
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
                final Value.Segment[] localSegments = store.get(key).getSegments(Ints.toArray(e.getValue()));
                for (final Value.Segment localSegment : localSegments) {
                    final int index = ArrayUtils.indexOf(segmentIndices, localSegment.segmentIndex);
                    segments[index] = localSegment;
                }
                logDHTAction(key, DHTAction.GET_SEGMENT);
                operationCompleteLatch.countDown();
            } else {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Value.Segment[] remoteSegments = getRemoteSegmentsBlocking(e.getKey(), key, Ints.toArray(e.getValue()));
                        for (final Value.Segment remoteSegment : remoteSegments) {
                            final int index = ArrayUtils.indexOf(segmentIndices, remoteSegment.segmentIndex);
                            segments[index] = remoteSegment;
                        }
                        operationCompleteLatch.countDown();
                    }
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

    private Value.Segment[] getRemoteSegmentsBlocking(final MachineDescriptor machine, final Key key, final int[] segmentIndices) {
        final CountDownLatch cdl = new CountDownLatch(1);
        final UUID requestID = UUID.randomUUID();
        requestTable.put(requestID, cdl);
        final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_GET_EVENT_SEGMENTS_REQUEST);
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
        final Key k = keyDirectory.get(key.internalUID);
        // At the moment we need a key with all the distribution metadata.
        Preconditions.checkState(k != null);
        // Iterate over keys' partition directory and either send
        // delete messages to remote partitions or delete locally.
        for (final Key.PartitionDescriptor pd : k.getPartitionDirectory().values())
            if (isLocal(pd.machine)) {
                // Local delete.
                if (keyDirectory.remove(key.internalUID) == null || store.remove(key) == null)
                    throw new IllegalStateException();
                logDHTAction(key, DHTAction.DELETE_VALUE);
            } else {
                // Remote delete.
                final NetEvents.NetEvent e = new NetEvents.NetEvent(DHT_DEL_EVENT);
                e.setPayload(key);
                netManager.sendEvent(pd.machine, e);
            }
    }

    // ---------------------------------------------------

    public Key getKey(final UUID internalID) {
        Preconditions.checkNotNull(internalID);
        final MachineDescriptor md = selectMachineForKey(internalID);
        if (isLocal(md)) {
            Key key = keyDirectory.get(internalID);
            while(key == null) {
                key = keyDirectory.get(internalID);
                if (key == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            return key;
        } else {
            final CountDownLatch cdl = new CountDownLatch(1);
            final UUID requestID = UUID.randomUUID();
            requestTable.put(requestID, cdl);
            final NetEvents.NetEvent event = new NetEvents.NetEvent(DHT_GET_EVENT_KEY_REQUEST);
            event.setPayload(Pair.of(requestID, internalID));
            netManager.sendEvent(md, event);
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
            return responseKeyTable.remove(requestID);
        }
    }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private MachineDescriptor selectMachineForKey(final Key key) { return selectMachineForKey(key.internalUID); }
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

    private enum DHTAction {
        PUT_VALUE,
        PUT_SEGMENT,
        GET_VALUE,
        GET_SEGMENT,
        DELETE_VALUE,
    }

    private void logDHTAction(final Key key, final DHTAction action) {
        LOG.info(action + " ON " + infraManager.getMachine() + " => KEY = "
                + key.internalUID + " | " + "PARTITION = " + key.getPartitionIndex());
    }
}
