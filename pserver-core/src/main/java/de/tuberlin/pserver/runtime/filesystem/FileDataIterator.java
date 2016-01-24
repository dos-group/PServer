package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.filesystem.records.Record;

import java.util.Iterator;

public interface FileDataIterator<T extends Record> extends Iterator<T> {

    void initialize();

    void reset();

    String getFilePath();

    FileSection getFileSection();
}
