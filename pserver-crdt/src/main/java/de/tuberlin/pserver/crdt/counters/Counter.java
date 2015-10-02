package de.tuberlin.pserver.crdt.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.DataManager;

public class Counter extends AbstractCounter implements CRDT {

    public Counter(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeID, IOperation op) {
        Operation<Integer> cop = (Operation<Integer>) op;

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
    public boolean add(int i) {
        if(addCount(i)) {
            broadcast(new Operation<>(CRDT.SUM, i), dataManager);
            return true;
        }
        return false;
    }

    public boolean subtract(int i) {
        if(subtractCount(i)) {
            broadcast(new Operation<>(CRDT.SUBTRACT, i), dataManager);
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
