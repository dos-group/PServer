package de.tuberlin.pserver.runtime.filesystem.record;

/**
 * Created by Morgan K. Geldenhuys on 17.12.15.
 */
public class SVMRecordIterator implements RecordIterator {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object next(long lineNumber) {
        return null;
    }
}
