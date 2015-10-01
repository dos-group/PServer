package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.IllegalOperationException;
import de.tuberlin.pserver.crdt.Operation;
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

        if(cop.getType() == CounterOperation.ADD) {
            return addCount(cop.getValue());
        }
        else if(cop.getType() == CounterOperation.SUBTRACT) {
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
            broadcast(new CounterOperation(CounterOperation.ADD, i), dataManager);
            return true;
        }
        return false;
    }

    public boolean subtract(int i, DataManager dataManager) {
        if(subtractCount(i)) {
            broadcast(new CounterOperation(CounterOperation.SUBTRACT, i), dataManager);
            return true;
        }
        return false;
    }

    public boolean subtractCount(int i) {
        count -= i; // = count - new Long(i);
        return true;
    }

    private boolean addCount(int i) {
        count += i; // = count + new Long(i);
        return true;
    }
}
