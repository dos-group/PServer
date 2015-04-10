package de.tuberlin.pserver.core.filesystem;

import java.util.Iterator;

public interface FileDataIterator<T> extends Iterator<T> {

    public abstract void initialize();

    public abstract void reset();
}
