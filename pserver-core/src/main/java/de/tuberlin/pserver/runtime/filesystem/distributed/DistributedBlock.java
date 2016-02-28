package de.tuberlin.pserver.runtime.filesystem.distributed;

import org.apache.hadoop.fs.BlockLocation;


public final class DistributedBlock {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long globalOffset;

    public final BlockLocation block;

    public final boolean isSplitBlock;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedBlock() { this(-1, null, false); }
    public DistributedBlock(long globalOffset, BlockLocation block, boolean isSplitBlock) {
        this.globalOffset = globalOffset;
        this.block = block;
        this.isSplitBlock = isSplitBlock;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedBlock that = (DistributedBlock) o;
        if (globalOffset != that.globalOffset) return false;
        if (isSplitBlock != that.isSplitBlock) return false;
        return block.equals(that.block);

    }

    @Override
    public int hashCode() {
        int result = (int) (globalOffset ^ (globalOffset >>> 32));
        result = 31 * result + block.hashCode();
        result = 31 * result + (isSplitBlock ? 1 : 0);
        return result;
    }
}
