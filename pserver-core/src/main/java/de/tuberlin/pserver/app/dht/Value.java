package de.tuberlin.pserver.app.dht;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.memory.Buffer;

import java.io.Serializable;
import java.util.UUID;

public class Value implements Serializable {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class Segment implements Serializable {

        public final int segmentIndex;

        public final byte[] data;

        public Segment(final int segmentIndex,
                       final byte[] data) {
            Preconditions.checkArgument(segmentIndex >= 0);
            this.segmentIndex = segmentIndex;
            this.data = Preconditions.checkNotNull(data);
        }
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1;

    public static final int MAX_SIZE = 20480 * Key.DEFAULT_SEGMENT_SIZE; // ~83MB

    public static final int MIN_SIZE = 2048 * Key.DEFAULT_SEGMENT_SIZE; // ~8.3MB

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final boolean allocateMemory;

    private Buffer data;

    /** The UID of the associated key. */
    private UUID internalUID;

    private transient Key key;

    private final int partitionSize;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private Value(final int partitionSize,
                  final boolean allocateMemory) {
        Preconditions.checkArgument(partitionSize > 0);
        this.partitionSize = partitionSize;
        this.allocateMemory = allocateMemory;
        this.data = allocateMemory ? new Buffer(partitionSize) : null;
    }

    // ---------------------------------------------------

    public static Value[] newValue(final boolean allocateMemory, final long size) {
        Preconditions.checkArgument(size > 0 && size % Key.DEFAULT_SEGMENT_SIZE == 0);
        Value[] valuePartitions;
        if (size > MAX_SIZE) {
            final int numberOfDHTNodes = DHT.getInstance().getNumberOfDHTNodes();
            int numPartitions = (int) Math.ceil((double) size / (double) MAX_SIZE);
            if (numPartitions <= numberOfDHTNodes) {
                long lastPartitionSize = size % MAX_SIZE;
                if (lastPartitionSize == 0)
                    lastPartitionSize = MAX_SIZE;
                else if (lastPartitionSize <= MIN_SIZE) {
                    numPartitions -= 1;
                    lastPartitionSize += MAX_SIZE;
                    Preconditions.checkState(lastPartitionSize < Integer.MAX_VALUE);
                }
                valuePartitions = new Value[numPartitions];
                for (int i = 0; i < numPartitions; ++i) {
                    final int s = (i < numPartitions - 1) ? MAX_SIZE : (int)lastPartitionSize;
                    valuePartitions[i] = new Value(s, allocateMemory);
                }
            } else {
                long partitionSize = size / numberOfDHTNodes;
                Preconditions.checkState(partitionSize < Integer.MAX_VALUE);
                int lastPartitionRemainder = ((int)size % numberOfDHTNodes) != 0 ? (int)size % MAX_SIZE : 0;
                valuePartitions = new Value[numberOfDHTNodes];
                for (int i = 0; i < numberOfDHTNodes; ++i) {
                    final int s = (i < numberOfDHTNodes - 1) ? (int)partitionSize : (int)partitionSize + lastPartitionRemainder;
                    valuePartitions[i] = new Value(s, allocateMemory);
                }
            }
        } else {
            valuePartitions = new Value[1];
            valuePartitions[0] = new Value((int)size, allocateMemory);
        }
        return valuePartitions;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setKey(final Key key) { this.key = Preconditions.checkNotNull(key); }

    public void setInternalUID(final UUID uid) {
        this.internalUID = Preconditions.checkNotNull(uid);
    }

    public byte[] getRawData() { return Preconditions.checkNotNull(data).getRawData(); }

    public void allocateMemory() {
        Preconditions.checkState(allocateMemory == false && data == null);
        data = new Buffer(Preconditions.checkNotNull(key).getPartitionDescriptor().partitionSize);
    }

    public boolean isAllocated() { return allocateMemory; }

    public int getPartitionSize() { return partitionSize; }

    public Segment[] getSegments(final int[] segmentIndices) {
        Preconditions.checkNotNull(segmentIndices);
        final Segment[] segments = new Segment[segmentIndices.length];
        int j = 0;
        final Key.PartitionDescriptor pd = key.getPartitionDescriptor();
        for (final int segmentIndex : segmentIndices) {
            final int normalizedIndex = segmentIndex - pd.segmentBaseIndex;
            final byte[] segmentData = new byte[pd.segmentSize];
            System.arraycopy(data.getRawData(), normalizedIndex * pd.segmentSize, segmentData, 0, pd.segmentSize);
            segments[j++] = new Segment(segmentIndex, segmentData);
        }
        return segments;
    }

    public void putSegments(final Segment[] segments) {
        Preconditions.checkNotNull(segments);
        final Key.PartitionDescriptor pd = key.getPartitionDescriptor();
        for (final Segment segment : segments) {
            final int normalizedIndex = segment.segmentIndex - pd.segmentBaseIndex;
            System.arraycopy(segment.data, 0, data.getRawData(), normalizedIndex * pd.segmentSize, pd.segmentSize);
        }
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {
        /*final Value[] vals = Value.newValue(false, Value.MAX_SIZE * 2 + 120);
        System.out.println("num of partitions: " + vals.length);
        int i = 0;
        for (final Value val : vals)
            System.out.println("[" + i++ + "] => " + val.getPartitionSize());*/
    }
}
