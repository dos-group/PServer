package de.tuberlin.pserver.runtime.filesystem.hdfs;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.config.IConfig;
import de.tuberlin.pserver.runtime.core.infra.MachineDescriptor;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSection;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;

import java.io.IOException;

public class HDFSFileDataIterator implements FileDataIterator<IRecord> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    //private static final Logger LOG = LoggerFactory.getLogger(HDFSFileDataIterator.class);

    private final MachineDescriptor machine;

    private final InputSplitProvider inputSplitProvider;

    private final HDFSInputFile inputFile;

    private FileInputSplit split;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public HDFSFileDataIterator(final IConfig config,
                                final MachineDescriptor machine,
                                final InputSplitProvider inputSplitProvider,
                                final HDFSInputFile inputFile) {

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
    public FileSection getFileSection() {
        return null;
    }

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
    public IRecord next() {
        try {
            return inputFile.nextRecord(null);
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
