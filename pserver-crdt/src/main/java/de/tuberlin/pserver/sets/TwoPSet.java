package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

/**
 * In a Two-Phase Set an element may be added and removed but never added again thereafter.
 */

public class TwoPSet<T> extends AbstractTwoPSet<T> {

    public TwoPSet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        SetOperation<T> sop = (SetOperation<T>)op;

        if(sop.getType() == SetOperation.ADD) {
            if(add(sop.getValue())) {
                return true;
            }
        }
        else if(sop.getType() == SetOperation.REMOVE) {
            if(remove(sop.getValue())) {
                return true;
            }
        }
        return false;
    }
}
