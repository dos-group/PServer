package de.tuberlin.pserver.app.filesystem;

public interface FileSystemManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String PSERVER_LFSM_COMPUTED_FILE_SPLITS  = "PSERVER_LFSM_COMPUTED_FILE_SPLITS";

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void computeInputSplitsForRegisteredFiles();

    public abstract <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType);
}
