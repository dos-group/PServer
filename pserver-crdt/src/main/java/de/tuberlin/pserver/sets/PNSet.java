package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

/**
 *
 * Maintain a counter for each element. The element is in the set if its count is > 0.
 */
// TODO: At the moment, negative values are allowed. Perhaps it would be good to give a choice for not allowing negative count.
public class PNSet<T> extends AbstractPNSet<T> {
    public PNSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        return false;
    }
}
