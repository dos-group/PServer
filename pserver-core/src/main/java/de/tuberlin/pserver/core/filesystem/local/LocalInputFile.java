package de.tuberlin.pserver.core.filesystem.local;

import de.tuberlin.pserver.core.filesystem.FileDataIterator;

public interface LocalInputFile<T> {

    public abstract void computeLocalFileSection(final int numNodes, final int nodeIdx);

    public abstract FileDataIterator<T> iterator();
}
