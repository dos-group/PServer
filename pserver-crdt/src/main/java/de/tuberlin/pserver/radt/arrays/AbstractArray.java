package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import org.apache.commons.collections.list.FixedSizeList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractArray<T> extends AbstractRADT<T> implements IArray<T> {
    // The array is implemented as a list to ensure type safety as there are no generic arrays in Java :/
    private final List<Item<T>> array;
    protected final int size;

    public AbstractArray(int size, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.size = size;

        // Initialize array
        array = new ArrayList<>();
        for(int i = 0; i < size; i++) {
            array.add(new Item<T>(-1, new S4Vector(nodeId, new int[0]), null));
        }
    }

    protected synchronized void set(int i, Item<T> item) {
        array.set(i, item);
    }

    protected synchronized Item<T> get(int i) {
        return array.get(i);
    }
}
