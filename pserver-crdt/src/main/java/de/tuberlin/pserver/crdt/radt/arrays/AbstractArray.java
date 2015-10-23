package de.tuberlin.pserver.crdt.radt.arrays;

import de.tuberlin.pserver.crdt.radt.AbstractRADT;
import de.tuberlin.pserver.crdt.radt.CObject;
import de.tuberlin.pserver.crdt.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;

import java.util.Arrays;

public abstract class AbstractArray<T> extends AbstractRADT<T> implements IArray<T> {
    protected final CObject<T>[] array;

    public AbstractArray(int size, String id, int noOfReplicas, RuntimeManager runtimeManager) {
        super(size, id, noOfReplicas, runtimeManager);

        // Initialize array
        array = new CObject[size];
        Arrays.fill(array, new CObject(-1, vectorClock, new S4Vector(0, 0, new int[0], 0), null));

    }
}
