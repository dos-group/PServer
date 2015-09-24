package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;
import java.util.EmptyStackException;

public class GCounter extends AbstractCounter implements CRDT, Serializable {

    public GCounter(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected void update(int srcNodeID, Operation op, DataManager dm) {
        CounterOperation cop = (CounterOperation) op;

      //  while(cop != null) {
            if (cop.getType() == CounterOperation.ADD) {
                count += cop.getValue();
            } else {
                // TODO: throw a specific exception
                throw new EmptyStackException();
            }
      //  }
    }
}
