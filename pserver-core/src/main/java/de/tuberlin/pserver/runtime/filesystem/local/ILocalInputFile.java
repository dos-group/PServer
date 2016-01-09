package de.tuberlin.pserver.runtime.filesystem.local;

import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.records.Record;

//public interface ILocalInputFile<T extends IRecord> {

public interface ILocalInputFile<T extends Record> {

    public abstract void computeLocalFileSection(final int numNodes, final int nodeIdx);

    public abstract FileDataIterator<T> iterator();
}
