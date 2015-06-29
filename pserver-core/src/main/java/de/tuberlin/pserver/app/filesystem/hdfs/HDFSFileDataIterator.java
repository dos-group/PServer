package de.tuberlin.pserver.app.filesystem.hdfs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.record.Record;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.infra.MachineDescriptor;

import java.io.IOException;

public class HDFSFileDataIterator implements FileDataIterator<Record> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //private static final Logger LOG = LoggerFactory.getLogger(HDFSFileDataIterator.class);

    private final MachineDescriptor machine;

    private final InputSplitProvider inputSplitProvider;

    private final HDFSCSVInputFile inputFile;

    private FileInputSplit split;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public HDFSFileDataIterator(final IConfig config,
                                final MachineDescriptor machine,
                                final InputSplitProvider inputSplitProvider,
                                final HDFSCSVInputFile inputFile) {

        Preconditions.checkNotNull(config);
        this.machine = Preconditions.checkNotNull(machine);
        this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
        this.inputFile = Preconditions.checkNotNull(inputFile);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void initialize() {
        try {
            split = (FileInputSplit)inputSplitProvider.getNextInputSplit(machine);
            if (split == null)
                throw new IllegalStateException();
            inputFile.open(split);
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void reset() { throw new UnsupportedOperationException(); }

    @Override
    public String getFilePath() { return inputFile.getFilePath().getName(); }

    @Override
    public boolean hasNext() {
        try {
            if (inputFile.reachedEnd()) {
                inputFile.close();
                split = (FileInputSplit)inputSplitProvider.getNextInputSplit(machine);
                if (split == null)
                    return false;
                inputFile.open(split);
            }
            return true;
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Record next() {
        try {
            return Record.wrap(inputFile.nextRecord(null), null);
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
