package de.tuberlin.pserver.runtime.filesystem.distributed;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterator;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Optional;

public class DistributedFileIterator implements AbstractFileIterator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final DistributedFilePartition partition;

    private FSDataInputStream inputStream;

    private RecordIterator recordIterator;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFileIterator(DistributedFile file) {
        this.partition = (DistributedFilePartition) Preconditions.checkNotNull(file).getFilePartition();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() {
        try {
            FileAccessThread fat = new FileAccessThread(partition, 150000);
            fat.start();
            inputStream = fat.waitForCompletion();
        } catch (Throwable t) {
            throw new IllegalStateException("Error opening distributed file partition. " +
                    "\nFile: " +   partition.file +
                    "\nOffset: " + partition.startOffset +
                    "\nSize: " +   partition.size + "\n : " + t.getMessage(), t);
        }

        if (partition.startOffset != 0) {
            try {
                this.inputStream.seek(partition.startOffset);
                this.inputStream.readLine();
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }

        recordIterator = RecordIterator.create(partition.fileFormat, inputStream, Optional.<long[]>empty());
    }

    @Override
    public boolean hasNext() {
        final boolean hasNext = recordIterator.hasNext();
        if (!hasNext)
            close();
        return hasNext;
    }

    @Override
    public Record next() { return recordIterator.next(); }

    @Override
    public void close() {
        try {
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    // ---------------------------------------------------
    // Private Inner Classes.
    // ---------------------------------------------------

    private static class FileAccessThread extends Thread {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final DistributedFilePartition partition;

        private volatile FSDataInputStream inputStream;

        private volatile Throwable error;

        private volatile boolean aborted;

        private final long timeout;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public FileAccessThread(DistributedFilePartition partition, long timeout) {
            super("Transient InputSplit Opener");
            setDaemon(true);
            this.partition = partition;
            this.timeout = timeout;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void run() {
            try {
                final FileSystem fs = FileSystem.get(partition.hdfsConfig);
                this.inputStream = fs.open(new Path(this.partition.file));
                // check for canceling and close the stream in that case, because no one will obtain it
                if (this.aborted) {
                    final FSDataInputStream f = this.inputStream;
                    this.inputStream = null;
                    f.close();
                }
            }
            catch (Throwable t) {
                this.error = t;
            }
        }

        public FSDataInputStream waitForCompletion() throws Throwable {
            final long start = System.currentTimeMillis();
            long remaining = this.timeout;
            do {
                try {
                    // wait for the task completion
                    this.join(remaining);
                } catch (InterruptedException iex) {
                    // we were canceled, so abort the procedure
                    abortWait();
                    throw iex;
                }
            } while(this.error == null
                    && this.inputStream == null
                    && (remaining = this.timeout + start - System.currentTimeMillis()) > 0);

            if (this.error != null)
                throw this.error;

            if (this.inputStream != null) {
                return this.inputStream;
            } else {
                // double-check that the stream has not been set by now. we don't know here whether
                // a) the opener thread recognized the canceling and closed the stream
                // b) the flag was set such that the stream did not see it and we have a valid stream
                // In any case, close the stream and throw an exception.
                abortWait();
                final boolean stillAlive = this.isAlive();
                final StringBuilder bld = new StringBuilder(256);
                for (StackTraceElement e : this.getStackTrace()) {
                    bld.append("\tat ").append(e.toString()).append('\n');
                }
                throw new IOException("Input opening request timed out. Opener was " + (stillAlive ? "" : "NOT ") +
                        " alive. Stack of split open thread:\n" + bld.toString());
            }
        }

        private void abortWait() {
            this.aborted = true;
            final FSDataInputStream inStream = this.inputStream;
            this.inputStream = null;
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Throwable t) {}
            }
        }
    }
}
