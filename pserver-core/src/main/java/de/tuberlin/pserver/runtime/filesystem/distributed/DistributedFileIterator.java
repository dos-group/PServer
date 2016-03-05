package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DistributedFileIterator implements AbstractFileIterator {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final long blockAccessTimeout = 150000;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private DistributedFile file;

    private RecordIterator recordIterator;

    private DistributedFileIterationContext ic;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFileIterator(DistributedFile file) {
        this.ic = new DistributedFileIterationContext((DistributedFilePartition) file.getFilePartition());
        this.file = file;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() { firstBlock(); }

    @Override
    public boolean hasNext() {
        checkForNextBlock();
        boolean hasNext = recordIterator.hasNext();
        if (!hasNext) {
            if (checkForNextBlock())
                hasNext = recordIterator.hasNext();
        }
        return hasNext;
    }

    @Override
    public Record next() { return recordIterator.next(); }

    @Override
    public void close() {
        try {
            if (ic.inputStream != null) {
                ic.inputStream.close();
                ic.inputStream = null;
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }


    //public Pair<Integer,Long>


    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void firstBlock() {
        try {
            boolean skipFirstLine = ic.partition.blocks.get(ic.blockSeqID).blockLoc.getOffset() != 0;
            FileAccessThread fat = new FileAccessThread(
                    ic.partition.hdfsConfig,
                    ic.partition.blocks.get(ic.blockSeqID),
                    skipFirstLine,
                    blockAccessTimeout
            );
            fat.start();
            ic.inputStream = fat.waitForCompletion();
            recordIterator = RecordIterator.create((MatrixTypeInfo) file.getTypeInfo(), ic);
            System.out.println("access first block => " + ic.partition.blocks.get(ic.blockSeqID).file);
        } catch (Throwable ex) {
            throw new IllegalStateException(ex);
        }
    }

    private boolean checkForNextBlock() {
        if (ic.blockSeqID + 1 == ic.partition.blocks.size())
            return false;

        try {

            if (ic.requireNextBlock()) {
                boolean skipFirstLine = ic.inputStream.getPos() != ic.getCurrentBlockEndOffset() && !ic.exceededBlock;

                ++ic.blockSeqID;
                try {
                    close();

                    FileAccessThread fat = new FileAccessThread(
                            ic.partition.hdfsConfig,
                            ic.partition.blocks.get(ic.blockSeqID),
                            skipFirstLine,
                            blockAccessTimeout
                    );
                    fat.start();

                    ic.exceededBlock = false;
                    ic.inputStream = fat.waitForCompletion();

                    System.out.println("Access next block [" + ic.blockSeqID + "] " +
                            "of file [" + ic.partition.blocks.get(ic.blockSeqID).file + "]");

                } catch (Throwable t) {
                    throw new IllegalStateException("Error opening distributed file partition. " +
                            "\nFile: "      + ic.partition.file +
                            "\nOffset: "    + ic.partition.startOffset +
                            "\nSize: "      + ic.partition.size + "\n : " + t.getMessage(), t);
                }
            }

        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
        return true;
    }

    // ---------------------------------------------------
    // Private Inner Classes.
    // ---------------------------------------------------

    private static class FileAccessThread extends Thread {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final Configuration hdfsConfig;

        private final DistributedBlock distBlock;

        private final boolean skipFirstLine;

        private volatile FSDataInputStream inputStream;

        private volatile Throwable error;

        private volatile boolean aborted;

        private final long timeout;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public FileAccessThread(Configuration hdfsConfig, DistributedBlock distBlock, boolean skipFirstLine, long timeout) {
            super("DistributedBlock-Opener-Thread");
            setDaemon(true);
            this.hdfsConfig     = hdfsConfig;
            this.distBlock      = distBlock;
            this.skipFirstLine  = skipFirstLine;
            this.timeout        = timeout;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void run() {
            try {
                FileSystem fs = FileSystem.get(hdfsConfig);
                inputStream = fs.open(new Path(distBlock.file));

                if (distBlock.blockLoc.getOffset() != 0) {
                    inputStream.seek(distBlock.blockLoc.getOffset());
                    if (skipFirstLine) {
                        System.out.println("SKIP first line of block [" + distBlock.blockSeqID + "].");
                        inputStream.readLine();
                    } else {
                        System.out.println("SKIP NOT first line of block [" + distBlock.blockSeqID + "].");
                    }
                }
                // check for canceling and close the stream in that case, because no one will obtain it
                if (this.aborted) {
                    final FSDataInputStream f = inputStream;
                    inputStream = null;
                    f.close();
                }
            }
            catch (Throwable t) {
                error = t;
            }
        }

        public FSDataInputStream waitForCompletion() throws Throwable {
            long start = System.currentTimeMillis();
            long remaining = this.timeout;
            do {
                try {
                    // wait for the task completion
                    join(remaining);
                } catch (InterruptedException iex) {
                    // we were canceled, so abort the procedure
                    abortWait();
                    throw iex;
                }
            } while(error == null
                    && inputStream == null
                    && (remaining = timeout + start - System.currentTimeMillis()) > 0);

            if (error != null)
                throw error;

            if (inputStream != null) {
                return inputStream;
            } else {
                // double-check that the stream has not been set by now. we don't know here whether
                // a) the opener thread recognized the canceling and closed the stream
                // b) the flag was set such that the stream did not see it and we have a valid stream
                // In any case, close the stream and throw an exception.
                abortWait();
                boolean stillAlive = this.isAlive();
                StringBuilder bld = new StringBuilder(256);
                for (StackTraceElement e : this.getStackTrace()) {
                    bld.append("\tat ").append(e.toString()).append('\n');
                }
                throw new IOException("Input opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") +
                        " alive. Stack of split open thread:\n" + bld.toString());
            }
        }

        private void abortWait() {
            aborted = true;
            FSDataInputStream inStream = this.inputStream;
            inputStream = null;
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Throwable t) {}
            }
        }
    }
}
