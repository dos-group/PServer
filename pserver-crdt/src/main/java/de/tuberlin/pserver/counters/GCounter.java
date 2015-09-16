package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;

public class GCounter extends AbstractCounter implements CRDT, Serializable {
    private int count = 0;

    public GCounter(int id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    public int getCount() {
        // Defensive copy so the value can not be changed without evoking the add method which broadcasts updates
        // TODO: Does this defensive copy make sense?
        int tmp = count;
        return tmp;
    }

    @Override
    protected void update(int srcNodeID, Operation op, DataManager dm) {
        if(op.getType() == ADD) {
            count += op.getValue();
        } else if(op.getType() == END) {
            addFinishedNode(srcNodeID, dm);
        } else {
            // TODO: throw a specific exception
        }
    }
}
