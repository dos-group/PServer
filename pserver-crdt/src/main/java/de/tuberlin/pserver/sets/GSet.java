package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

public class GSet<T> extends AbstractGSet {

    public GSet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected void update(int srcNodeId, Operation op, DataManager dm) {
        SetOperation<T> sop = (SetOperation<T>)op;

        if(sop.getType() == SetOperation.ADD) {
            value.add(sop.getValue());
        }
    }
}
