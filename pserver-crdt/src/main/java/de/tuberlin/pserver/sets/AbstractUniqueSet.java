package de.tuberlin.pserver.sets;


import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.sets.exceptions.NotUniqueException;

public abstract class AbstractUniqueSet<T> extends AbstractSet<T> {

    public AbstractUniqueSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }

    protected boolean add(T item) {
        if(!value.add(item)) {
            throw new NotUniqueException("The value "+ item + " is already contained in the Unique Set and cannot be " +
                    "added again.");
        }
        return true;
    }

    protected boolean remove(T item) {
        return value.remove(item);
    }
}
