package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartition;
import de.tuberlin.pserver.types.typeinfo.properties.FileFormat;

import java.util.List;

public final class DistributedFilePartition extends AbstractFilePartition {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //public final Configuration hdfsConfig;

    public final String hdfsHome;

    public final String hdfsURL;

    public final long startOffset;

    public final long size;

    public final List<DistributedBlock> blocks;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFilePartition() { this(null, null, -1,  null, null, -1, -1, null); }
    public DistributedFilePartition(String hdfsHome,
                                    String hdfsURL,
                                    int nodeID,
                                    String file,
                                    FileFormat fileFormat,
                                    long startOffset,
                                    long size,
                                    List<DistributedBlock> blocks) {

        super(nodeID, file, fileFormat);
        //this.hdfsConfig     = hdfsConfig;
        this.hdfsHome       = hdfsHome;
        this.hdfsURL        = hdfsURL;
        this.startOffset    = startOffset;
        this.size           = size;
        this.blocks         = blocks;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedFilePartition that = (DistributedFilePartition) o;
        if (size != that.size) return false;
        return file.equals(that.file);

    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + (int) (size ^ (size >>> 32));
        return result;
    }

    @Override
    public String toString() { return "num of blocks = " + blocks.size(); }
}
