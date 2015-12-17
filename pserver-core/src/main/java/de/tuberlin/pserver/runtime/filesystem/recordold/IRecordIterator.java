package de.tuberlin.pserver.runtime.filesystem.recordold;

public interface IRecordIterator {

    boolean hasNext();

    IRecord next(long lineNumber);
}
