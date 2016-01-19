package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.filesystem.records.Record;

import java.util.Iterator;

/*public interface FileDataIterator<T extends IRecord> extends Iterator<T> {

    public abstract void initialize();

    public abstract void reset();

    public abstract String getFilePath();

    public abstract FileSection getFileSection();
}*/

public interface FileDataIterator<T extends Record> extends Iterator<T> {

    void initialize();

    void reset();

    String getFilePath();

    FileSection getFileSection();
}
