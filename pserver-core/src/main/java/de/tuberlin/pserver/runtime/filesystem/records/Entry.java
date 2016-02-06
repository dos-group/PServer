package de.tuberlin.pserver.runtime.filesystem.records;

import java.io.Serializable;

public class Entry<V extends Number> implements Serializable {

    private long row;
    private long col;
    private V value;

    public Entry(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }

    public long getRow() {
        return this.row;
    }

    public long getCol() {
        return this.col;
    }

    public V getValue() {
        return this.value;
    }

    public String toString() {
        return this.col + ":" + this.value;
    }

    public Entry<V> set(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
        return this;
    }

}
