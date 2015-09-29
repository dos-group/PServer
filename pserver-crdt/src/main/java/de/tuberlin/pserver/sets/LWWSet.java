package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

/**
 * An element is in the set if it is in the add-Set and not in the remove-Set with a higher timestamp.
 */
// TODO: what about if this grows infinitely until it is too large for memory? Manual Garbage collection somehow?
public class LWWSet<T> extends AbstractLWWSet<T> {

    public LWWSet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        SetOperation<T> lwws = (SetOperation<T>) op;

        if(lwws.getType() == SetOperation.ADD) {
            return add(lwws);
        }
        else if(lwws.getType() == SetOperation.REMOVE) {
            return remove(lwws);
        }
        else {
            return false;
        }
    }
}
