package de.tuberlin.pserver.app.dht.valuetypes;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class AbstractBufferValue extends Value {

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

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBufferValue.class);

    private static final long serialVersionUID = -1;

    protected final boolean allocateMemory;

    protected final int partitionSize;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractBufferValue(final int partitionSize,
                               final boolean allocateMemory) {

        Preconditions.checkArgument(partitionSize > 0);
        this.partitionSize  = partitionSize;
        this.allocateMemory = allocateMemory;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void allocateMemory(final int instanceID);

    public abstract void compress();

    public abstract void decompress();

    public abstract Segment[] getSegments(final int[] segmentIndices, final int instanceID);

    public abstract void putSegments(final Segment[] segments, final int instanceID);

    // ---------------------------------------------------

    public boolean isAllocated() { return allocateMemory; }

    public int getPartitionSize() { return partitionSize; }
}
