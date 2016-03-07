package de.tuberlin.pserver.runtime.filesystem.distributed;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;
import de.tuberlin.pserver.types.matrix.typeinfo.MatrixTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedFileIterator implements AbstractFileIterator {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String FILE_SYSTEM_BLOCK_REQUEST = "file_system_block_request";

    public static final String FILE_SYSTEM_BLOCK_RESPONSE = "file_system_block_response";

    public static final String FILE_SYSTEM_RETURN_REMAINING_BLOCKS = "file_system_return_remaining_blocks";

    private static final long BLOCK_ACCESS_TIMEOUT = 150000;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int fileSystemMaster;

    private final NetManager netManager;

    private final DistributedFile file;

    private RecordIterator recordIterator;

    private DistributedFileIterationContext ic;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFileIterator(Config config, NetManager netManager, DistributedFile file) {
        this.ic = new DistributedFileIterationContext((DistributedFilePartition) file.getFilePartition());
        this.fileSystemMaster = config.getInt(FileSystemManager.FILE_MASTER_NODE_ID);
        this.netManager = netManager;
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
        if (!hasNext && checkForNextBlock())
            hasNext = recordIterator.hasNext();
        if (!hasNext) {
            List<DistributedBlock> remainingBlocks = collectRemainingBlocks();
            netManager.dispatchEventAt(
                    new int[] { fileSystemMaster },
                    new NetEvent(FILE_SYSTEM_RETURN_REMAINING_BLOCKS, remainingBlocks)
            );
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

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void firstBlock() {
        try {
            boolean skipFirstLine = ic.partition.blocks.get(ic.localBlockID).blockLoc.getOffset() != 0;
            FileAccessThread fat = new FileAccessThread(
                    ic.partition.hdfsConfig,
                    ic.partition.blocks.get(ic.localBlockID),
                    skipFirstLine,
                    BLOCK_ACCESS_TIMEOUT
            );
            fat.start();
            ic.inputStream = fat.waitForCompletion();
            recordIterator = RecordIterator.create((MatrixTypeInfo) file.getTypeInfo(), ic);
            System.out.println("access first block => " + ic.partition.blocks.get(ic.localBlockID).file);
        } catch (Throwable ex) {
            throw new IllegalStateException(ex);
        }
    }

    private boolean checkForNextBlock() {
        if (ic.localBlockID + 1 == ic.partition.blocks.size())
            requestRemoteBlock();
        try {
            if (ic.requireNextBlock()) {
                boolean skipFirstLine = ic.inputStream.getPos() != ic.getCurrentBlockEndOffset() && !ic.exceededBlock;
                ++ic.localBlockID;
                try {
                    close();
                    FileAccessThread fat = new FileAccessThread(
                            ic.partition.hdfsConfig,
                            ic.partition.blocks.get(ic.localBlockID),
                            skipFirstLine,
                            BLOCK_ACCESS_TIMEOUT
                    );
                    fat.start();
                    ic.exceededBlock = false;
                    ic.inputStream = fat.waitForCompletion();
                    System.out.println("Access next block [" + ic.localBlockID + "] " +
                            "of file [" + ic.partition.blocks.get(ic.localBlockID).file + "]");
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

    private List<DistributedBlock> collectRemainingBlocks() {
        List<DistributedBlock> remainingBlocks = new ArrayList<>();
        try {
            if (ic.exceededBlock) {
                if (!(ic.localBlockID + 1 < ic.partition.blocks.size())) {
                    DistributedBlock db = new DistributedBlock(
                            ic.partition.blocks.get(ic.localBlockID).file,
                            ic.localBlockID,
                            ic.partition.blocks.get(ic.localBlockID).blockLoc,
                            ic.inputStream.getPos(),
                            true
                    );
                    remainingBlocks.add(db);
                }
                if (ic.localBlockID + 1 < ic.partition.blocks.size())
                    remainingBlocks.addAll(ic.partition.blocks.subList(ic.localBlockID + 1, ic.partition.blocks.size()));
            }
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
        return remainingBlocks;
    }

    private void requestRemoteBlock() {
        final CountDownLatch awaitBlockResponse = new CountDownLatch(1);
        netManager.addEventListener(FILE_SYSTEM_BLOCK_RESPONSE, (event) -> {
            ic.partition.blocks.add(Preconditions.checkNotNull((DistributedBlock) event.getPayload()));
            awaitBlockResponse.countDown();
        } );
        // Request a block and wait.
        netManager.dispatchEventAt(new int[] { fileSystemMaster }, new NetEvent(FILE_SYSTEM_BLOCK_REQUEST));
        try {
            awaitBlockResponse.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            if (ic.localBlockID + 1 == ic.partition.blocks.size())
                throw new IllegalStateException(e);
        }
        netManager.removeEventListener(FILE_SYSTEM_BLOCK_RESPONSE);
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

                if (distBlock.blockLoc.getOffset() != 0 && !distBlock.isSplitBlock) {
                    inputStream.seek(distBlock.blockLoc.getOffset());
                    if (skipFirstLine) {
                        System.out.println("SKIP first line of block [" + distBlock.blockSeqID + "].");
                        inputStream.readLine();
                    } else
                        System.out.println("SKIP NOT first line of block [" + distBlock.blockSeqID + "].");
                } else {
                    if (distBlock.isSplitBlock)
                        inputStream.seek(distBlock.splitOffset);
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
                    join(remaining);
                } catch (InterruptedException iex) {
                    abortWait();
                    throw iex;
                }
            } while(error == null && inputStream == null
                    && (remaining = timeout + start - System.currentTimeMillis()) > 0);

            if (error != null)
                throw error;

            if (inputStream != null)
                return inputStream;
            else {
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
