package de.tuberlin.pserver.runtime.filesystem.records;

import java.util.Iterator;


public interface Record extends Iterator<RecordEntry32F> {

    int size();

    void setLabel(float label);

    float getLabel();

    long getRow();

    RecordEntry32F next();

    Record set(long row, String line);
}
