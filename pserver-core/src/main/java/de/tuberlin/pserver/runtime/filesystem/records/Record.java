package de.tuberlin.pserver.runtime.filesystem.records;

import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;

public class Record {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int DEFAULT_RECORD_CAPACITY = 50;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public int row;

    public float label;

    public TIntFloatMap entries;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Record() {
        this.label   = Float.NaN;
        this.entries = new TIntFloatHashMap(DEFAULT_RECORD_CAPACITY);
    }
}
