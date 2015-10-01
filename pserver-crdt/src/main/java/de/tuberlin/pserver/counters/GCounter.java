package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.IllegalOperationException;
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
    protected boolean update(int srcNodeID, Operation op, DataManager dm) {
        CounterOperation cop = (CounterOperation) op;

        if (cop.getType() == CounterOperation.ADD) {
            return addCount(cop.getValue());
        } else {
            // TODO: message text
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

    private boolean addCount(long l) {
        if(l > 0) {
            count += l;
            return true;
        }
        else {
           // TODO: throw an exception because l < 0
           // throw new exception
            return false;
        }
    }
}
