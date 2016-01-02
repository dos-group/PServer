package de.tuberlin.pserver.runtime.state.entries;

import java.util.Map;

/**
 * Created by hegemon on 01.01.16.
 */
public class ImmutableSVMEntry<V extends Number> extends SVMEntry<V> {

    public ImmutableSVMEntry(Map<Integer, V> attributes) {
        super(attributes);
    }

    public ImmutableSVMEntry(SVMEntry svmEntry) {
        super(svmEntry.attributes);
    }

    @Override
    public V getValue(int i) {
        return this.attributes.get(i);
    }

}
