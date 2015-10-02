package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.*;

/**
 *
 * The OR ISet doesn't make any fucking sense in my opinion... At least not the way it is proposed and implemented here
 */
public class ORSet<T> extends AbstractSet<T> {
    private Map<T,List<UUID>> map = new HashMap<>();

    public ORSet(String id, DataManager dataManager) {
        super(id, dataManager);
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, IOperation<T> op) {
        TaggedOperation<T,UUID> sop = (TaggedOperation<T,UUID>) op;

        if(op.getType() == CRDT.ADD) {
            return addElement(sop.getValue(), sop.getTag());
        }
        else if(op.getType() == CRDT.REMOVE) {
            return removeElement(sop.getValue(), sop.getTag());
        }
        else {
            throw new IllegalOperationException("This operation is not supported by ORSet");
        }
    }

    @Override
    public boolean add(T value) {
        UUID id = UUID.randomUUID();

        if(addElement(value, id)) {
            broadcast(new TaggedOperation<>(CRDT.ADD, value, id), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T value) {
        UUID id = getId(value);

        if(removeElement(value, id)) {
            broadcast(new TaggedOperation<>(CRDT.REMOVE, value, id), dataManager);
            return true;
        }
        return false;
    }

    private boolean addElement(T value, UUID id) {
        final ArrayList<UUID> list;

        System.out.println("Add: " + value + ", " + id);
        if(map.get(value) == null) {
            list = new ArrayList<>();
            list.add(id);
            map.put(value, list);
        }
        else {
            map.get(value).add(id);
        }
        return true;
    }

    private boolean removeElement(T value, UUID id) {
        System.out.println("Remove: " + value + ", " + id);
        boolean a = map.get(value).remove(id);

        if(map.get(value).size() == 0) { map.remove(value);}
        return a;
    }

   /* public boolean add(TaggedOperation<T> sop) {
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

    public boolean remove(TaggedOperation<T> sop) {
        System.out.println("Remove: " + sop.getValue() + ", " + sop.getId());
        if(map.get(sop.getValue()) != null) {
            boolean a = map.get(sop.getValue()).remove(sop.getId());

            if(map.get(sop.getValue()).size() == 0) { map.remove(sop.getValue());}
            return a;
        }

        return false;
    }*/

    public Set<T> getSet() {
        return map.keySet();
    }

    public UUID getId(T value) {
        if(map.get(value) != null) {
            return map.get(value).get(0);
        }

        return null;
    }
}