package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.core.common.Deactivatable;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.MatrixPartitioner;

public interface FileSystemManager extends Deactivatable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_LFSM_COMPUTED_FILE_SPLITS  = "PSERVER_LFSM_COMPUTED_FILE_SPLITS";

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void computeInputSplitsForRegisteredFiles();

    public abstract <T extends Record> FileDataIterator<T> createFileIterator(
            final String filePath,
            final FileFormat fileFormat,
            final MatrixPartitioner partitioner);

    public abstract void clearContext();
}
