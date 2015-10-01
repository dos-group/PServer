package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.IllegalOperationException;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.*;

/**
 *
 * The OR Set doesn't make any fucking sense in my opinion... At least not the way it is proposed and implemented here
 */
public class ORSet<T> extends AbstractORSet<T> {
    private Map<T,List<UUID>> map = new HashMap<>();

    public ORSet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        SetOperation<T> sop = (SetOperation<T>) op;

        if(op.getType() == SetOperation.ADD) {
            return add(sop);
        }
        else if(op.getType() == SetOperation.REMOVE) {
            return remove(sop);
        }
        else {
            throw new IllegalOperationException("This operation is not supported by ORSet");
        }
    }

    public boolean add(SetOperation<T> sop) {
        System.out.println("Add: " + sop.getValue() + ", " + sop.getId());
        if(map.get(sop.getValue()) == null) {
            ArrayList<UUID> list = new ArrayList<>();
            list.add(sop.getId());
            map.put(sop.getValue(), list);
        }
        else {
            map.get(sop.getValue()).add(sop.getId());
        }
        return true;
    }

    public boolean remove(SetOperation<T> sop) {
        System.out.println("Remove: " + sop.getValue() + ", " + sop.getId());
        if(map.get(sop.getValue()) != null) {
            boolean a = map.get(sop.getValue()).remove(sop.getId());

            if(map.get(sop.getValue()).size() == 0) { map.remove(sop.getValue());}
            return a;
        }

        return false;
    }

    public Set<T> getSet() {

        return map.keySet();
    }

    public UUID getId(T value) {
        if(map.get(value) != null) {
            return map.get(value).get(0);
        }

        return null;
    }

    public SetOperation<T> getOperation(T value, int type) {
        return null;
    }
}
