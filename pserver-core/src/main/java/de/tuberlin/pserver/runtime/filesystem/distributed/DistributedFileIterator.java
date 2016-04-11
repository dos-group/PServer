package de.tuberlin.pserver.runtime.filesystem.distributed;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.SVMRecordParser;
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

    public static final String FILE_SYSTEM_COLLECT_BLOCKS = "file_system_return_remaining_blocks";

    private static final long BLOCK_ACCESS_TIMEOUT = 150000;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Configuration hdfsConfig;

    private final int fileSystemMaster;

    private final NetManager netManager;

    private final SVMRecordParser recordParser;

    private final DistributedFileIterationContext ic;

    private final MatrixTypeInfo matrixTypeInfo;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFileIterator(Config config, NetManager netManager, DistributedFile file) {
        this.ic = new DistributedFileIterationContext((DistributedFilePartition) file.getFilePartition());

        this.hdfsConfig = new Configuration();
        this.hdfsConfig.set("fs.defaultFS",     ic.partition.hdfsURL);
        this.hdfsConfig.set("HADOOP_HOME",      ic.partition.hdfsHome);
        this.hdfsConfig.set("hadoop.home.dir",  ic.partition.hdfsHome);
        System.setProperty("HADOOP_HOME",       ic.partition.hdfsHome);
        System.setProperty("hadoop.home.dir",   ic.partition.hdfsHome);

        this.netManager = netManager;
        this.fileSystemMaster = config.getInt(FileSystemManager.FILE_MASTER_NODE_ID);
        this.recordParser = new SVMRecordParser(ic);
        this.matrixTypeInfo = (MatrixTypeInfo)file.getTypeInfo();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() { fetchNextBlock(); }

    @Override
    public boolean hasNext() {
        boolean hasNext = matrixTypeInfo.rows() > ic.row;
        if (hasNext) {
            try {
                if (ic.requireNextBlock()) {
                    if (ic.localBlockID + 1 >= ic.partition.blocks.size()) {
                        requestRemoteBlock();
                    }
                    fetchNextBlock();
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            List<DistributedBlock> remainingBlocks = collectRemainingBlocks();
            System.out.println("COLLECT REMAINING BLOCKS => " + remainingBlocks.size());
            netManager.dispatchEventAt(
                    new int[] { fileSystemMaster },
                    new NetEvent(FILE_SYSTEM_COLLECT_BLOCKS, remainingBlocks)
            );
        }
        return hasNext;
    }

    @Override
    public Record next() {
        Record record = recordParser.parseNextRow(ic.row);
        ++ic.row;
        return record;
    }

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

    private void initInputStream(DistributedBlock block) throws Exception {
        ic.currentFile = block.file;
        if (block.isSplitBlock) {
            ic.inputStream.seek(block.splitOffset);
        } else {
            if (block.offset != 0) {
                ic.inputStream.seek(block.offset - 1);
                if (((char) ic.inputStream.read()) != '\n')
                    ic.inputStream.readLine();
            }
        }
    }

    private void fetchNextBlock() {
        try {
            close();
            ++ic.localBlockID;
            DistributedBlock block = ic.partition.blocks.get(ic.localBlockID);
            FileAccessThread fat = new FileAccessThread(hdfsConfig, block, BLOCK_ACCESS_TIMEOUT);
            fat.start();
            ic.inputStream = fat.waitForCompletion();
            initInputStream(block);
            System.out.println("ACCESS NEXT BLOCK [" + ic.localBlockID + "] " + "OF FILE [" + ic.partition.blocks.get(ic.localBlockID).file + "]");
        } catch (Throwable t) {
            throw new IllegalStateException("Error opening distributed file partition. " +
                    "\nFile: "      + ic.partition.file +
                    "\nOffset: "    + ic.partition.startOffset +
                    "\nSize: "      + ic.partition.size + "\n : " + t.getMessage(), t);
        }
    }

    private List<DistributedBlock> collectRemainingBlocks() {
        List<DistributedBlock> remainingBlocks = new ArrayList<>();
        try {
            if (ic.inputStream.getPos() < ic.getCurrentBlockEndOffset()) {
                DistributedBlock db = new DistributedBlock(
                        ic.partition.blocks.get(ic.localBlockID).file,
                        ic.partition.blocks.get(ic.localBlockID).blockSeqID,
                        ic.partition.blocks.get(ic.localBlockID).isCorrupt,
                        ic.partition.blocks.get(ic.localBlockID).offset,
                        ic.partition.blocks.get(ic.localBlockID).length,
                        ic.inputStream.getPos(),
                        true
                );
                remainingBlocks.add(db);
            }
            if (ic.localBlockID + 1 < ic.partition.blocks.size())
                remainingBlocks.addAll(ic.partition.blocks.subList(ic.localBlockID + 1, ic.partition.blocks.size()));
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
        System.out.println("REQUEST REMOTE BLOCK");
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

        private volatile FSDataInputStream inputStream;

        private volatile Throwable error;

        private volatile boolean aborted;

        private final long timeout;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public FileAccessThread(Configuration hdfsConfig, DistributedBlock distBlock, long timeout) {
            super("DistributedBlock-Opener-Thread");
            setDaemon(true);
            this.hdfsConfig     = hdfsConfig;
            this.distBlock      = distBlock;
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
