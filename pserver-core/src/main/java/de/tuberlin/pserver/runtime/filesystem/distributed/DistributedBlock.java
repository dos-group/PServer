package de.tuberlin.pserver.runtime.filesystem.distributed;

import de.tuberlin.pserver.runtime.filesystem.AbstractBlock;


public final class DistributedBlock implements AbstractBlock {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String file;

    public final long blockSeqID;

    public final boolean isCorrupt;

    //public final BlockLocation blockLoc;

    public final long offset;

    public final long length;

    // ---------------------------------------------------

    public final boolean isSplitBlock;

    public final long splitOffset;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedBlock() { this(null, -1, false, -1, -1, -1, false); }
    //public DistributedBlock(String file, long blockSeqID, long offset, long length) { this(file, blockSeqID, -1, -1, -1, false); }
    public DistributedBlock(String file, long blockSeqID, boolean isCorrupt, long offset, long length, long splitOffset, boolean isSplitBlock) {
        this.file           = file;
        this.blockSeqID     = blockSeqID;
        this.isCorrupt      = isCorrupt;
        this.offset         = offset;
        this.length         = length;
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
        if (offset != that.offset) return false;
        if (isSplitBlock != that.isSplitBlock) return false;
        if (splitOffset != that.splitOffset) return false;
        return file.equals(that.file);

    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + (int) (blockSeqID ^ (blockSeqID >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "[file = " + file + ", blockSeqID = " + blockSeqID
                + ", offset = " + offset + ", length = " + length + "]";
    }
}
