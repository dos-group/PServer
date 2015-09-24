package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;

public class Counter extends AbstractCounter implements CRDT, Serializable {

    public Counter(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected void update(int srcNodeID, Operation op, DataManager dm) {
        CounterOperation cop = (CounterOperation) op;
        if(cop.getType() == CounterOperation.ADD) {
            count += cop.getValue();
        }
        else if(cop.getType() == CounterOperation.SUBTRACT) {
            count -= cop.getValue();
        }
        else {
            // TODO: throw a specific exception
        }
    }
}
