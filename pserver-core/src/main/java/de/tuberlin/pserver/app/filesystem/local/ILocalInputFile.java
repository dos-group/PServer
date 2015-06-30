package de.tuberlin.pserver.app.filesystem.local;

import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.record.IRecord;

public interface ILocalInputFile<T extends IRecord> {

    public abstract void computeLocalFileSection(final int numNodes, final int nodeIdx);

    public abstract FileDataIterator<T> iterator();
}
