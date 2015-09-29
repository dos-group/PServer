package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGSet<T> extends AbstractSet<T> {

    public AbstractGSet(String id, DataManager dataManager) {
        super(id, dataManager);
    }
}
