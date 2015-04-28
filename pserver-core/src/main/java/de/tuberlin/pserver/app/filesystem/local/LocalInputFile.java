package de.tuberlin.pserver.app.filesystem.local;

import de.tuberlin.pserver.app.filesystem.FileDataIterator;

public interface LocalInputFile<T> {

    public abstract void computeLocalFileSection(final int numNodes, final int nodeIdx);

    public abstract FileDataIterator<T> iterator();
}
