package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.runtime.DataManager;

public abstract class AbstractORSet<T> extends AbstractSet<T>{

    public AbstractORSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }
}
