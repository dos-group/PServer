package de.tuberlin.pserver.runtime.filesystem.record;

public interface IRecordIterator {

    boolean hasNext();

    IRecord next(long row);

}
