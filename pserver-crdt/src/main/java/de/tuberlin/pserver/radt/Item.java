package de.tuberlin.pserver.radt;

public class Item<T> extends CObject<T> {
    private final int index;

    public Item(int index, int[] vectorClock, S4Vector s4Vector, T value) {
        super(vectorClock, s4Vector, value);
        this.index = index;
    }


    public int getIndex() {
        return index;
    }
}
