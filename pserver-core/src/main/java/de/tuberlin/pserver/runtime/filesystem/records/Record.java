package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.state.matrix.entries.Entry;
import de.tuberlin.pserver.runtime.state.matrix.entries.ReusableEntry;

import java.util.Iterator;
import java.util.Optional;


public interface Record<V extends Number> extends Iterator<Entry> {

    int size();

    void setLabel(V label);

    V getLabel();

    long getRow();

    Entry next();

    Entry next(ReusableEntry reusableEntry);

    Record set(long row, String line, Optional<long[]> projection);
}
