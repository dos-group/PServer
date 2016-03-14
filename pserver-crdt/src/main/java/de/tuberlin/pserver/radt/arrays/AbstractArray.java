package de.tuberlin.pserver.radt.arrays;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// The array is implemented as a list to ensure type safety as there are no generic arrays in Java :/
public abstract class AbstractArray<T> extends AbstractRADT<T> implements IArray<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<Element<T>> array;

    protected final int size;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractArray(int size, String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        this.size = size;

        int[] vectorClock = new int[noOfReplicas];

        Arrays.fill(vectorClock, 0);

        // Initialize array
        array = new ArrayList<>();

        for(int i = 0; i < size; i++) {

            array.add(new Element<>(null, new S4Vector(nodeId, vectorClock)));

        }

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected synchronized void set(int i, Element<T> item) {

        array.set(i, item);

    }

    protected synchronized Element<T> get(int i) {

        return array.get(i);

    }

}
