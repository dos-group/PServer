package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.filesystem.records.Record;

import java.util.Iterator;

public interface AbstractFileIterator extends Iterator<Record> {

    void open();

    void close();
}
