package de.tuberlin.pserver.runtime.filesystem.records;

import java.util.Iterator;
import java.util.Optional;


public interface Record extends Iterator<Entry32F> {

    int size();

    void setLabel(float label);

    float getLabel();

    long getRow();

    Entry32F next();

    Entry32F next(Entry32F reusableEntry);

    Record set(long row, String line, Optional<long[]> projection);
}
