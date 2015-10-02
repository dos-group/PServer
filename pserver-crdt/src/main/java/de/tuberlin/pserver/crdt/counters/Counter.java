package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.CounterOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;

public class Counter extends AbstractCounter implements CRDT, Serializable {

    public Counter(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeID, Operation op, DataManager dm) {
        CounterOperation cop = (CounterOperation) op;

        if(cop.getType() == CRDT.SUM) {
            return addCount(cop.getValue());
        }
        else if(cop.getType() == CRDT.SUBTRACT) {
            return subtractCount(cop.getValue());
        }
        else {
            // TODO: throw a specific exception message
            throw new IllegalOperationException("Blub");
        }
    }

    @Override
    public boolean add(int i, DataManager dataManager) {
        if(addCount(i)) {
            broadcast(new CounterOperation(CRDT.SUM, i), dataManager);
            return true;
        }
        return false;
    }

    public boolean subtract(int i, DataManager dataManager) {
        if(subtractCount(i)) {
            broadcast(new CounterOperation(CRDT.SUBTRACT, i), dataManager);
            return true;
        }
        return false;
    }

    public boolean subtractCount(long i) {
        count -= i; // = count - new Long(i);
        return true;
    }

    private boolean addCount(long i) {
        count += i; // = count + new Long(i);
        return true;
    }
}
