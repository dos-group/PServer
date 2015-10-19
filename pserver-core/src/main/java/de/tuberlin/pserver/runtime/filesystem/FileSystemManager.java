package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.core.common.Deactivatable;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIteratorProducer;
import de.tuberlin.pserver.runtime.partitioning.partitioner.IMatrixPartitioner;

public interface FileSystemManager extends Deactivatable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_LFSM_COMPUTED_FILE_SPLITS  = "PSERVER_LFSM_COMPUTED_FILE_SPLITS";

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void computeInputSplitsForRegisteredFiles();

    public abstract <T extends IRecord> FileDataIterator<T> createFileIterator(
                            final String filePath,
                            final IRecordIteratorProducer recordFormat,
                            final IMatrixPartitioner partitioner);

    public abstract void clearContext();
}
