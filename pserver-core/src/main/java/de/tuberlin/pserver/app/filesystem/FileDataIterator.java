package de.tuberlin.pserver.app.filesystem;

import de.tuberlin.pserver.app.filesystem.record.IRecord;

import java.util.Iterator;

public interface FileDataIterator<T extends IRecord> extends Iterator<T> {

    public abstract void initialize();

    public abstract void reset();

    public abstract String getFilePath();
}
