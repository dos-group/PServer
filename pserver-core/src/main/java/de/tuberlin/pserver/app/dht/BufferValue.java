package de.tuberlin.pserver.app.dht;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.experimental.memory.Buffer;

import java.io.Serializable;

public class BufferValue extends Value {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class Segment implements Serializable {

        public final int segmentIndex;

        public final byte[] data;

        public Segment(final int segmentIndex,
                       final byte[] data) {
            this.segmentIndex = segmentIndex;
            this.data = Preconditions.checkNotNull(data);
        }
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int DEFAULT_ALIGNMENT_SIZE = 8; // (in bytes)

    public static final int DEFAULT_SEGMENT_SIZE = 4096; // (in bytes)

    public static final int MAX_SIZE = 20480 * DEFAULT_SEGMENT_SIZE; // ~83MB

    public static final int MIN_SIZE = 2048 * DEFAULT_SEGMENT_SIZE; // ~8.3MB

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1;

    protected final boolean allocateMemory;

    protected final int partitionSize;

    protected Buffer buffer;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public BufferValue(final int partitionSize,
                       final boolean allocateMemory) {
        Preconditions.checkArgument(partitionSize > 0);
        this.partitionSize = partitionSize;
        this.allocateMemory = allocateMemory;
        // TODO: this.buffer = allocateMemory ? new Buffer(partitionSize) : null;
    }

    // ---------------------------------------------------

    /*public static BufferValue compressBufferValue(final Buffer.CompressionPolicy compressionPolicy,
                                                  final BufferValue bufferValue) {
        Preconditions.checkNotNull(compressionPolicy);
        Preconditions.checkNotNull(bufferValue);
        final BufferValue compressedValue = new BufferValue(bufferValue.partitionSize, bufferValue.allocateMemory);
        compressedValue.buffer = Buffer.compressBuffer(compressionPolicy, bufferValue.getBuffer());
        return compressedValue;
    }*/

    /** Creates no copy! */
    /*public static BufferValue decompressBufferValue(final BufferValue bufferValue) {
        Preconditions.checkNotNull(bufferValue);
        bufferValue.setBuffer(Buffer.decompressBuffer(bufferValue.getBuffer()));
        return bufferValue;
    }*/

    // ---------------------------------------------------

    public static BufferValue[] newValue(final boolean allocateMemory, long size) {
        Preconditions.checkArgument(size > 0 && size % DEFAULT_SEGMENT_SIZE == 0);
        BufferValue[] valuePartitions;
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
                valuePartitions = new BufferValue[numPartitions];
                for (int i = 0; i < numPartitions; ++i) {
                    final int s = (i < numPartitions - 1) ? MAX_SIZE : (int)lastPartitionSize;
                    valuePartitions[i] = new BufferValue(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            } else {
                long partitionSize = size / numberOfDHTNodes;
                Preconditions.checkState(partitionSize < Integer.MAX_VALUE);
                int lastPartitionRemainder = ((int)size % numberOfDHTNodes) != 0 ? (int)size % MAX_SIZE : 0;
                valuePartitions = new BufferValue[numberOfDHTNodes];
                for (int i = 0; i < numberOfDHTNodes; ++i) {
                    final int s = (i < numberOfDHTNodes - 1) ? (int)partitionSize : (int)partitionSize + lastPartitionRemainder;
                    valuePartitions[i] = new BufferValue(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            }
        } else {
            valuePartitions = new BufferValue[1];
            valuePartitions[0] = new BufferValue((int)size, allocateMemory);
            //if (valuePartitions[0].isAllocated()) valuePartitions[0].allocateMemory();
        }

        return valuePartitions;
    }

    // ---------------------------------------------------

    public static BufferValue[] newValueAligned(final boolean allocateMemory, final long size, final long alignmentSize) {

        Preconditions.checkState(size > MAX_SIZE);
        Preconditions.checkState(DEFAULT_SEGMENT_SIZE % alignmentSize == 0
                || alignmentSize % DEFAULT_SEGMENT_SIZE == 0);
        Preconditions.checkState(alignmentSize <= MAX_SIZE);
        Preconditions.checkState(size % alignmentSize == 0);

        BufferValue[] valuePartitions = null;
        final int max_size = (int)((MAX_SIZE / alignmentSize) * alignmentSize);
        if (size > max_size) {
            final int numberOfDHTNodes = DHT.getInstance().getNumberOfDHTNodes();
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
                valuePartitions = new BufferValue[numPartitions];
                for (int i = 0; i < numPartitions; ++i) {
                    final int s = (i < numPartitions - 1) ? max_size : (int)lastPartitionSize;
                    valuePartitions[i] = new BufferValue(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            } else {
                long partitionSize = size / numberOfDHTNodes;
                partitionSize = (partitionSize / alignmentSize) * alignmentSize;
                Preconditions.checkState(partitionSize < Integer.MAX_VALUE);
                long lastPartitionRemainder = size % partitionSize;
                valuePartitions = new BufferValue[numberOfDHTNodes];
                for (int i = 0; i < numberOfDHTNodes; ++i) {
                    final int s = (i < numberOfDHTNodes - 1) ? (int)partitionSize : (int)(partitionSize + lastPartitionRemainder);
                    valuePartitions[i] = new BufferValue(s, allocateMemory);
                    //if (valuePartitions[i].isAllocated()) valuePartitions[0].allocateMemory();
                }
            }
        }

        return valuePartitions;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setBuffer(final Buffer buffer) { this.buffer = Preconditions.checkNotNull(buffer); }

    public Buffer getBuffer() { return buffer; }

    public void setRawData(final byte[] data) { this.buffer.setRawData(Preconditions.checkNotNull(data)); }

    public byte[] getRawData() { return Preconditions.checkNotNull(buffer).getRawData(); }

    public boolean isAllocated() { return allocateMemory; }

    public int getPartitionSize() { return partitionSize; }

    public void allocateMemory(final int instanceID) {
        Preconditions.checkState(allocateMemory == false && buffer == null);
        buffer = new Buffer(Preconditions.checkNotNull(key).getPartitionDescriptor(instanceID).partitionSize);
    }

    public Segment[] getSegments(final int[] segmentIndices, final int instanceID) {
        Preconditions.checkNotNull(segmentIndices);
        final Segment[] segments = new Segment[segmentIndices.length];
        int j = 0;
        final Key.PartitionDescriptor pd = key.getPartitionDescriptor(instanceID);
        for (final int segmentIndex : segmentIndices) {
            final int normalizedIndex = segmentIndex - pd.segmentBaseIndex;
            final byte[] segmentData = new byte[pd.segmentSize];
            System.arraycopy(buffer.getRawData(), normalizedIndex * pd.segmentSize, segmentData, 0, pd.segmentSize);
            segments[j++] = new Segment(segmentIndex, segmentData);
        }
        return segments;
    }

    public void putSegments(final Segment[] segments, final int instanceID) {
        Preconditions.checkNotNull(segments);
        final Key.PartitionDescriptor pd = key.getPartitionDescriptor(instanceID);
        for (final Segment segment : segments) {
            final int normalizedIndex = segment.segmentIndex - pd.segmentBaseIndex;
            System.arraycopy(segment.data, 0, buffer.getRawData(), normalizedIndex * pd.segmentSize, pd.segmentSize);
        }
    }
}
