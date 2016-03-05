package de.tuberlin.pserver.runtime.filesystem.distributed;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;


public final class DistributedBlock {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String file;

    public final long blockSeqID;

    public final long globalOffset;

    public final BlockLocation blockLoc;

    public final boolean isSplitBlock;

    public final Path path;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedBlock() { this(null, null, -1, null, -1, false); }
    public DistributedBlock(Path path, String file, long blockSeqID, BlockLocation blockLoc) { this(path, file, blockSeqID, blockLoc, -1, false); }
    public DistributedBlock(Path path, String file, long blockSeqID, BlockLocation blockLoc, long globalOffset, boolean isSplitBlock) {
        this.path           = path;
        this.file           = file;
        this.blockSeqID     = blockSeqID;
        this.globalOffset   = globalOffset;
        this.blockLoc       = blockLoc;
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
        if (globalOffset != that.globalOffset) return false;
        if (isSplitBlock != that.isSplitBlock) return false;
        if (!file.equals(that.file)) return false;
        return blockLoc.equals(that.blockLoc);

    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + (int) (blockSeqID ^ (blockSeqID >>> 32));
        result = 31 * result + (int) (globalOffset ^ (globalOffset >>> 32));
        result = 31 * result + blockLoc.hashCode();
        result = 31 * result + (isSplitBlock ? 1 : 0);
        return result;
    }
}
