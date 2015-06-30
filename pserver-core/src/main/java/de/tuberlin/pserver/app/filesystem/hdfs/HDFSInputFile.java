package de.tuberlin.pserver.app.filesystem.hdfs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.record.Record;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.net.NetManager;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class HDFSInputFile implements InputFormat<Record,FileInputSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSInputFile.class);

    private static final long serialVersionUID = 1L;

    private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

    protected static final long READ_WHOLE_SPLIT_FLAG = -1L;

    // --------------------------------------------------------------------------------------------
    //  Variables for internal operation.
    // --------------------------------------------------------------------------------------------

    private transient final IConfig config;

    private transient final NetManager netManager;

    protected transient FSDataInputStream stream;

    protected transient long splitStart;

    protected transient long splitLength;

    protected transient RecordFormat format;

    protected transient CSVParser csvFileParser;

    protected transient Iterator<CSVRecord> csvIterator;

    // --------------------------------------------------------------------------------------------
    //  The configuration parameters. Configured on the instance and serialized to be shipped.
    // --------------------------------------------------------------------------------------------

    protected  String path;

    protected Path filePath;

    protected long minSplitSize = 0;

    protected int numSplits = -1;

    protected boolean unsplittable = false;

    protected Configuration conf = new Configuration();

    // --------------------------------------------------------------------------------------------
    //  Constructors
    // --------------------------------------------------------------------------------------------

    public HDFSInputFile(final IConfig config, final NetManager netManager, final String filePath, RecordFormat format) {
        this.config     = Preconditions.checkNotNull(config);
        this.path       = Preconditions.checkNotNull(filePath);
        this.filePath   = new Path(filePath);
        this.netManager = Preconditions.checkNotNull(netManager);
        this.format     = Preconditions.checkNotNull(format);
    }

    public HDFSInputFile(final IConfig config, final NetManager netManager, final String filePath) {
        this(config, netManager, filePath, RecordFormat.DEFAULT);
    }

    // --------------------------------------------------------------------------------------------
    //  Getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    public Path getFilePath() { return filePath; }

    public void setFilePath(String filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path may not be null.");
        }
        // TODO The jobDescriptor-submission web interface passes empty args (and thus empty
        // paths) to compute the preview graph. The following is a workaround for
        // this situation and we should fix this.
        if (filePath.isEmpty()) {
            setFilePath(new Path(""));
            return;
        }
        setFilePath(new Path(filePath));
    }

    public void setFilePath(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("File path may not be null.");
        }
        this.filePath = filePath;
    }

    public long getMinSplitSize() { return minSplitSize; }

    public void setMinSplitSize(long minSplitSize) {
        if (minSplitSize < 0) {
            throw new IllegalArgumentException("The minimum split length cannot be negative.");
        }
        this.minSplitSize = minSplitSize;
    }

    public int getNumSplits() { return numSplits; }

    public void setNumSplits(int numSplits) {
        if (numSplits < -1 || numSplits == 0) {
            throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
        }
        this.numSplits = numSplits;
    }

    // --------------------------------------------------------------------------------------------
    // Getting information about the split that is currently open
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the start of the current split.
     *
     * @return The start of the split.
     */
    public long getSplitStart() { return splitStart; }

    /**
     * Gets the length or remaining length of the current split.
     *
     * @return The length or remaining length of the current split.
     */
    public long getSplitLength() { return splitLength; }

    // --------------------------------------------------------------------------------------------
    //  Pre-flight: Configuration, Splits, Sampling
    // --------------------------------------------------------------------------------------------

    public void configure(Configuration parameters) {
        conf = parameters;
    }

    public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] splits) {
        return new LocatableInputSplitAssigner(splits);
    }

    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }
        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);
        final Path path = this.filePath;
        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);
        // get all the files that are involved in the splits
        List<FileStatus> files = new ArrayList<FileStatus>();
        long totalLength = 0;
        final FileSystem fs = path.getFileSystem(conf);
        final FileStatus pathFile = fs.getFileStatus(path);
        if(!acceptFile(pathFile)) {
            throw new IOException("The given file does not pass the file-filter");
        }
        if (pathFile.isDir()) {
            // input is directory. list all contained files
            final FileStatus[] dir = fs.listStatus(path);
            for (int i = 0; i < dir.length; i++) {
                if (!dir[i].isDir() && acceptFile(dir[i])) {
                    files.add(dir[i]);
                    totalLength += dir[i].getLen();
                }
            }
        } else {

            files.add(pathFile);
            totalLength += pathFile.getLen();
        }

        // returns if unsplittable
        if(unsplittable) {
            int splitNum = 0;
            for (final FileStatus file : files) {
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
                Set<String> hosts = new HashSet<String>();
                for(BlockLocation block : blocks) {
                    hosts.addAll(Arrays.asList(block.getHosts()));
                }
                long len = file.getLen();
                FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), 0, len,
                        hosts.toArray(new String[hosts.size()]));
                inputSplits.add(fis);
            }
            return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
        }

        final long maxSplitSize = (minNumSplits < 1) ? Long.MAX_VALUE : (totalLength / minNumSplits +
                (totalLength % minNumSplits == 0 ? 0 : 1));
        // now that we have the files, generate the splits
        int splitNum = 0;
        for (final FileStatus file : files) {
            final long len = file.getLen();
            final long blockSize = file.getBlockSize();
            final long minSplitSize;
            if (this.minSplitSize <= blockSize) {
                minSplitSize = this.minSplitSize;
            }
            else {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Minimal split length of " + this.minSplitSize + " is larger than the block length of " +
                            blockSize + ". Decreasing minimal split length to block length.");
                }
                minSplitSize = blockSize;
            }

            final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
            final long halfSplit = splitSize >>> 1;
            final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
            if (len > 0) {
                // get the block locations and make sure they are in order with respect to their offset
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
                Arrays.sort(blocks, new Comparator<BlockLocation>() {

                    @Override
                    public int compare(BlockLocation o1, BlockLocation o2) {
                        if (o1.getOffset() < o2.getOffset()) {
                            return -1;
                        } else {
                            if (o1.getOffset() < o2.getOffset()) {
                                return 1;
                            } else
                                return 0;
                        }
                    }
                });

                long bytesUnassigned = len;
                long position = 0;
                int blockIndex = 0;

                while (bytesUnassigned > maxBytesForLastSplit) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), position, splitSize,
                            blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                    position += splitSize;
                    bytesUnassigned -= splitSize;
                }

                if (bytesUnassigned > 0) {
                    blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                    final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), position,
                            bytesUnassigned, blocks[blockIndex].getHosts());
                    inputSplits.add(fis);
                }
            } else {
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
                String[] hosts;
                if (blocks.length > 0)
                    hosts = blocks[0].getHosts();
                else
                    hosts = new String[0];

                final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath().toString(), 0, 0, hosts);
                inputSplits.add(fis);
            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }

    protected boolean acceptFile(FileStatus fileStatus) {
        final String name = fileStatus.getPath().getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }

    private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
        // go over all indexes after the startIndex
        for (int i = startIndex; i < blocks.length; i++) {
            long blockStart = blocks[i].getOffset();
            long blockEnd = blockStart + blocks[i].getLength();

            if (offset >= blockStart && offset < blockEnd) {
                // got the block where the split starts
                // check if the next block contains more than this one does
                if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
                    return i + 1;
                } else {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("The given offset is not contained in the any block.");
    }

    // --------------------------------------------------------------------------------------------

    public void open(FileInputSplit split) throws IOException {
        if (!(split instanceof FileInputSplit)) {
            throw new IllegalArgumentException("File Input Formats can only be used with FileInputSplits.");
        }
        final FileInputSplit fileSplit = (FileInputSplit) split;
        this.splitStart = fileSplit.getStart();
        this.splitLength = fileSplit.getLength();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening input split " + fileSplit.getPath() + " [" + this.splitStart + "," + this.splitLength + "]");
        }
        final InputSplitOpenThread isot = new InputSplitOpenThread(conf, fileSplit, 20000);
        isot.start();
        try {
            this.stream = isot.waitForCompletion();
        } catch (Throwable t) {
            throw new IOException("Error opening the Input Split " + fileSplit.getPath() +
                    " [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
        }
        if (this.splitStart != 0) {
            this.stream.seek(this.splitStart);
        }

        this.stream.readLine();
        this.csvFileParser = new CSVParser(new InputStreamReader(this.stream), format.getCsvFormat());
        this.csvIterator = csvFileParser.iterator();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !csvIterator.hasNext();
    }

    @Override
    public Record nextRecord(Record reuse) throws IOException {
        if(reuse == null) {
            return Record.wrap(csvIterator.next(), format.getProjection());
        }
        return reuse.set(csvIterator.next(), format.getProjection());
    }

    public void close() throws IOException {
        if (this.stream != null) {
            this.stream.close();
            stream = null;
        }
    }

    public String toString() {
        return this.filePath == null ?
                "File Input (unknown file)" :
                "File Input (" + this.filePath.toString() + ')';
    }

    public FileDataIterator<Record> iterator(final InputSplitProvider isp) {
        return new HDFSFileDataIterator(
                config,
                netManager.getMachineDescriptor(),
                Preconditions.checkNotNull(isp),
                this
        );
    }

    // ============================================================================================

    public static class InputSplitOpenThread extends Thread {

        private final FileInputSplit split;

        private volatile FSDataInputStream fdis;

        private volatile Throwable error;

        private volatile boolean aborted;

        private final Configuration conf;

        private final long timeout;

        public InputSplitOpenThread(Configuration conf, FileInputSplit split, long timeout) {
            super("Transient InputSplit Opener");
            setDaemon(true);

            this.conf = conf;

            this.split = split;

            this.timeout = timeout;
        }

        @Override
        public void run() {
            try {
                final FileSystem fs = FileSystem.get(conf);
                this.fdis = fs.open(new Path(this.split.getPath()));

                // check for canceling and close the stream in that case, because no one will obtain it
                if (this.aborted) {
                    final FSDataInputStream f = this.fdis;
                    this.fdis = null;
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
                }
                catch (InterruptedException iex) {
                    // we were canceled, so abort the procedure
                    abortWait();
                    throw iex;
                }
            }
            while (this.error == null && this.fdis == null &&
                    (remaining = this.timeout + start - System.currentTimeMillis()) > 0);

            if (this.error != null) {
                throw this.error;
            }
            if (this.fdis != null) {
                return this.fdis;
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

        private final void abortWait() {
            this.aborted = true;
            final FSDataInputStream inStream = this.fdis;
            this.fdis = null;
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Throwable t) {}
            }
        }
    }
}
