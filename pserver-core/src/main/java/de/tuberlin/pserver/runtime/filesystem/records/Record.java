package de.tuberlin.pserver.runtime.filesystem.records;

import java.util.Iterator;
import java.util.Optional;

public interface Record<V extends Number> extends Iterator<Entry> {

    int size();

    void setLabel(V label);

    V getLabel();

    long getRow();

    Entry next();

    Entry next(Entry entry);

    Record set(long row, String line, Optional<long[]> projection);
}
