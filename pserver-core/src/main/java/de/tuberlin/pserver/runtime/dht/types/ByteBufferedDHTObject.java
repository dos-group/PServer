package de.tuberlin.pserver.runtime.dht.types;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.dht.DHTKey;
import de.tuberlin.pserver.runtime.dht.DHTManager;
import de.tuberlin.pserver.runtime.memory.ManagedBuffer;

public class ByteBufferedDHTObject extends AbstractBufferedDHTObject {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected ManagedBuffer buffer;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ByteBufferedDHTObject(final int partitionSize,
                                 final boolean allocateMemory) {

        super(partitionSize, allocateMemory);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setBuffer(final ManagedBuffer buffer) { this.buffer = Preconditions.checkNotNull(buffer); }

    public ManagedBuffer getBuffer() { return buffer; }

    // ---------------------------------------------------

    @Override
    public void compress() { buffer.compress(); }

    @Override
    public void decompress() { buffer.decompress(); }

    @Override
    public void allocateMemory(final int nodeID) {
        Preconditions.checkState(allocateMemory == false && buffer == null);
        buffer = new ManagedBuffer(Preconditions.checkNotNull(key).getPartitionDescriptor(nodeID).partitionSize);
    }

    @Override
    public Segment[] getSegments(final int[] segmentIndices, final int nodeID) {
        Preconditions.checkNotNull(segmentIndices);
        final Segment[] segments = new Segment[segmentIndices.length];
        int j = 0;
        final DHTKey.PartitionDescriptor pd = key.getPartitionDescriptor(nodeID);
        for (final int segmentIndex : segmentIndices) {
            final int normalizedIndex = segmentIndex - pd.segmentBaseIndex;
            final byte[] segmentData = new byte[pd.segmentSize];
            System.arraycopy(buffer.getRawData(), normalizedIndex * pd.segmentSize, segmentData, 0, pd.segmentSize);
            segments[j++] = new Segment(segmentIndex, segmentData);
        }
        return segments;
    }

    @Override
    public void putSegments(final Segment[] segments, final int nodeID) {
        Preconditions.checkNotNull(segments);
        final DHTKey.PartitionDescriptor pd = key.getPartitionDescriptor(nodeID);
        for (final Segment segment : segments) {
            final int normalizedIndex = segment.segmentIndex - pd.segmentBaseIndex;
            System.arraycopy(segment.data, 0, buffer.getRawData(), normalizedIndex * pd.segmentSize, pd.segmentSize);
        }
    }

    // ---------------------------------------------------

    public static ByteBufferedDHTObject[] newValue(final boolean allocateMemory, long size) {
        Preconditions.checkArgument(size > 0 && size % DEFAULT_SEGMENT_SIZE == 0);
        ByteBufferedDHTObject[] valuePartitions;
        if (size > MAX_SIZE) {
            final int numberOfDHTNodes = DHTManager.getInstance().getNumberOfDHTNodes();
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
                valuePartitions = new ByteBufferedDHTObject[numPartitions];
                for (int i = 0; i < numPartitions; ++i) {
                    final int s = (i < numPartitions - 1) ? MAX_SIZE : (int)lastPartitionSize;
                    valuePartitions[i] = new ByteBufferedDHTObject(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            } else {
                long partitionSize = size / numberOfDHTNodes;
                Preconditions.checkState(partitionSize < Integer.MAX_VALUE);
                int lastPartitionRemainder = ((int)size % numberOfDHTNodes) != 0 ? (int)size % MAX_SIZE : 0;
                valuePartitions = new ByteBufferedDHTObject[numberOfDHTNodes];
                for (int i = 0; i < numberOfDHTNodes; ++i) {
                    final int s = (i < numberOfDHTNodes - 1) ? (int)partitionSize : (int)partitionSize + lastPartitionRemainder;
                    valuePartitions[i] = new ByteBufferedDHTObject(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            }
        } else {
            valuePartitions = new ByteBufferedDHTObject[1];
            valuePartitions[0] = new ByteBufferedDHTObject((int)size, allocateMemory);
            //if (valuePartitions[0].isAllocated()) valuePartitions[0].allocateMemory();
        }

        return valuePartitions;
    }

    // ---------------------------------------------------

    public static ByteBufferedDHTObject[] newValueAligned(final boolean allocateMemory, final long size, final long alignmentSize) {

        Preconditions.checkState(size > MAX_SIZE);
        Preconditions.checkState(DEFAULT_SEGMENT_SIZE % alignmentSize == 0
                || alignmentSize % DEFAULT_SEGMENT_SIZE == 0);
        Preconditions.checkState(alignmentSize <= MAX_SIZE);
        Preconditions.checkState(size % alignmentSize == 0);

        ByteBufferedDHTObject[] valuePartitions = null;
        final int max_size = (int)((MAX_SIZE / alignmentSize) * alignmentSize);
        if (size > max_size) {
            final int numberOfDHTNodes = DHTManager.getInstance().getNumberOfDHTNodes();
            int numPartitions = (int) Math.ceil((double) size / (double) max_size);
            if (numPartitions <= numberOfDHTNodes) {
                long lastPartitionSize = size % max_size;
                if (lastPartitionSize == 0)
                    lastPartitionSize = max_size;
                else if (lastPartitionSize <= max_size) {
                    numPartitions -= 1;
                    lastPartitionSize += max_size;
                    Preconditions.checkState(lastPartitionSize < Integer.MAX_VALUE);
                }
                valuePartitions = new ByteBufferedDHTObject[numPartitions];
                for (int i = 0; i < numPartitions; ++i) {
                    final int s = (i < numPartitions - 1) ? max_size : (int)lastPartitionSize;
                    valuePartitions[i] = new ByteBufferedDHTObject(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            } else {
                long partitionSize = size / numberOfDHTNodes;
                partitionSize = (partitionSize / alignmentSize) * alignmentSize;
                Preconditions.checkState(partitionSize < Integer.MAX_VALUE);
                long lastPartitionRemainder = size % partitionSize;
                valuePartitions = new ByteBufferedDHTObject[numberOfDHTNodes];
                for (int i = 0; i < numberOfDHTNodes; ++i) {
                    final int s = (i < numberOfDHTNodes - 1) ? (int)partitionSize : (int)(partitionSize + lastPartitionRemainder);
                    valuePartitions[i] = new ByteBufferedDHTObject(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            }
        }

        return valuePartitions;
    }
}
