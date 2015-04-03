package de.tuberlin.pserver.core.filesystem;

public interface FileSystemManager {

    public abstract void computeInputSplitsForRegisteredFiles();

    public abstract <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType);
}
