package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.Arrays;

public abstract class AbstractArray<T> extends AbstractRADT<T> implements IArray<T> {
    protected final Item<T>[] array;

    public AbstractArray(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(id, noOfReplicas, runtimeManager);

        // Initialize array
        array = new Item[size];
        Arrays.fill(array, new Item<>(-1, vectorClock, new S4Vector(0, 0, new int[0], 0), null));
    }
}
