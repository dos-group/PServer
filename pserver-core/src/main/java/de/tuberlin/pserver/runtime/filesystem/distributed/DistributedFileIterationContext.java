package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.InputStream;

public class DistributedFileIterationContext extends AbstractFileIterationContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final DistributedFilePartition partition;

    public FSDataInputStream inputStream;

    public int localBlockID;

    public String currentFile;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFileIterationContext(DistributedFilePartition partition) {
        this.partition      = partition;
        this.inputStream    = null;
        this.localBlockID   = -1;
        this.row            = 0;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long getCurrentBlockStartOffset() { return partition.blocks.get(localBlockID).offset; }
    public long getCurrentBlockLength() { return partition.blocks.get(localBlockID).length; }
    public long getCurrentBlockEndOffset() { return getCurrentBlockStartOffset() + partition.blocks.get(localBlockID).length; }
    public boolean requireNextBlock() throws Exception { return inputStream.getPos() >= getCurrentBlockEndOffset(); }

    // ---------------------------------------------------

    @Override
    public InputStream getInputStream() { return inputStream; }

    @Override
    public int readNext() throws Exception {
        return inputStream.read();
    }
}
