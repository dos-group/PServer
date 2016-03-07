package de.tuberlin.pserver.runtime.filesystem.distributed;

import de.tuberlin.pserver.runtime.filesystem.AbstractBlock;
import org.apache.hadoop.fs.BlockLocation;


public final class DistributedBlock implements AbstractBlock {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String file;

    public final long blockSeqID;

    public final BlockLocation blockLoc;

    // ---------------------------------------------------

    public final boolean isSplitBlock;

    public final long splitOffset;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedBlock() { this(null, -1, null, -1, false); }
    public DistributedBlock(String file, long blockSeqID, BlockLocation blockLoc) { this(file, blockSeqID, blockLoc, -1, false); }
    public DistributedBlock(String file, long blockSeqID, BlockLocation blockLoc, long splitOffset, boolean isSplitBlock) {
        this.file           = file;
        this.blockSeqID     = blockSeqID;
        this.blockLoc       = blockLoc;
        this.splitOffset    = splitOffset;
        this.isSplitBlock   = isSplitBlock;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedBlock that = (DistributedBlock) o;
        if (blockSeqID != that.blockSeqID) return false;
        if (splitOffset != that.splitOffset) return false;
        if (isSplitBlock != that.isSplitBlock) return false;
        if (!file.equals(that.file)) return false;
        return blockLoc.equals(that.blockLoc);

    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + (int) (blockSeqID ^ (blockSeqID >>> 32));
        result = 31 * result + (int) (splitOffset ^ (splitOffset >>> 32));
        result = 31 * result + blockLoc.hashCode();
        result = 31 * result + (isSplitBlock ? 1 : 0);
        return result;
    }
}
