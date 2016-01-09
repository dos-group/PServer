package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.state.entries.Entry;
import de.tuberlin.pserver.runtime.state.entries.ReusableEntry;

import java.util.Iterator;
import java.util.Optional;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public interface Record extends Iterator<Entry> {

    int size();

    int getTarget();

    long getRow();

    Entry next();

    Entry next(ReusableEntry reusableEntry);

    Record set(long row, String line, Optional<long[]> projection);

}
