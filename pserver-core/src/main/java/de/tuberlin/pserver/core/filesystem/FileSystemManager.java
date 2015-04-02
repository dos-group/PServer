package de.tuberlin.pserver.core.filesystem;

public interface FileSystemManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum FileSystemType {

        FILE_SYSTEM_LOCAL,

        FILE_SYSTEM_HDFS;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void computeInputSplitsForRegisteredFiles();

    public abstract <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType);
}
