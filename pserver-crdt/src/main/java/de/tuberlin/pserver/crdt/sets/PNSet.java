package de.tuberlin.pserver.crdt.sets;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * Maintain a counter for each element. The element is in the set if its count is > 0.
 */
// TODO: At the moment, negative values are allowed. Perhaps it would be good to give a choice for not allowing negative count.
public class PNSet<T> extends AbstractSet<T> {
    private final Map<T, Integer> counter;

    public PNSet(String id, DataManager dataManager) {
        super(id, dataManager);
        counter = new HashMap<>();
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, IOperation<T> op) {
        Operation<T> sop = (Operation<T>) op;

        if(sop.getType() == CRDT.ADD) {
            return addElement(sop.getValue());
        }
        else if(sop.getType() == CRDT.REMOVE) {
            return removeElement(sop.getValue());
        }
        else {
            throw new IllegalOperationException("The operation " + op.getType() + " cannot be applied to a PNSet");
        }
    }

    @Override
    public boolean add(T element) {
        if(addElement(element)) {
            broadcast(new Operation<T>(CRDT.ADD, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(T element) {
        if(removeElement(element)) {
            broadcast(new Operation<T>(CRDT.REMOVE, element), dataManager);
            return true;
        }
        return false;
    }

    @Override
    public Set<T> getSet() {
        return counter.keySet().stream().filter(key -> counter.get(key) > 0).collect(Collectors.toSet());
    }

    private boolean addElement(T element) {
        Integer count = counter.get(element);

        if(count == null) {
            counter.put(element, 1);
        }
        else {
            counter.put(element, ++count);
        }
        return true;
    }

    private boolean removeElement(T element) {
        Integer count = counter.get(element);

        if(count == null) {
            counter.put(element, -1);
        }
        else {
            counter.put(element, --count);
        }
        return true;
    }
}