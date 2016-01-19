package de.tuberlin.pserver.runtime.state.entries;

/**
 * Created by hegemon on 01.01.16.
 */
public abstract class EntryImpl<V extends Number> implements Entry<V> {

    protected long row;
    protected long col;
    protected V value;

    public EntryImpl(long row, long col, V value) {
        this.row = row;
        this.col = col;
        this.value = value;
    }

    @Override
    public long getRow() {
        return this.row;
    }

    @Override
    public long getCol() {
        return this.col;
    }

    @Override
    public V getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.col + ":" + this.value;
    }

}
