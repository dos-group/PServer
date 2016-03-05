package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.InputStream;

public class DistributedFileIterationContext implements AbstractFileIterationContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final DistributedFilePartition partition;

    public FSDataInputStream inputStream;

    public int blockSeqID;

    public boolean exceededBlock;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFileIterationContext(DistributedFilePartition partition) {
        this.partition      = partition;
        this.inputStream    = null;
        this.blockSeqID     = 0;
        this.exceededBlock  = false;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public boolean checkAndSetBlockExceeded() throws Exception {
        exceededBlock = inputStream.getPos() > getCurrentBlockEndOffset();
        return exceededBlock;
    }

    public long getCurrentBlockStartOffset() {
        return partition.blocks.get(blockSeqID).blockLoc.getOffset();
    }

    public long getCurrentBlockLength() {
        return partition.blocks.get(blockSeqID).blockLoc.getLength();
    }

    public long getCurrentBlockEndOffset() {
        return getCurrentBlockStartOffset() + partition.blocks.get(blockSeqID).blockLoc.getLength();
    }

    public boolean requireNextBlock() throws Exception {
        return inputStream.getPos() >= getCurrentBlockEndOffset();
    }

    // ---------------------------------------------------

    @Override
    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public int readNext() throws Exception {
        int c = inputStream.read();
        checkAndSetBlockExceeded();
        return c;
    }
}
