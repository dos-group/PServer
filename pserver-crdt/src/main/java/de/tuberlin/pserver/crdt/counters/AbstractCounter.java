package de.tuberlin.pserver.crdt.counters;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.crdt.AbstractCRDT;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractCounter extends AbstractCRDT implements Counter {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long count;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractCounter(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        this.count = 0;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized long getCount() {

        return count;

    }


    @Override
    public synchronized long increment(int i) {

        Preconditions.checkArgument(i > 0, "The method increment() can not be invoked with 0 or a negative value: " + i);

        count = Math.addExact(count, i);

        return count;

    }

    @Override
    public synchronized long decrement(int i) {

        Preconditions.checkArgument(i > 0, "The method decrement() can not be invoked with 0 or a negative value: " + i);

        count = Math.subtractExact(count, i);

        return count;

    }

}
