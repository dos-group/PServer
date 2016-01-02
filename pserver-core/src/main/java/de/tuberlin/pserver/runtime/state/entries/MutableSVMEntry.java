package de.tuberlin.pserver.runtime.state.entries;

import java.util.Map;

/**
 * Created by hegemon on 01.01.16.
 */
public class MutableSVMEntry<V extends Number> extends SVMEntry<V> implements ReusableSVMEntry<V> {

    public MutableSVMEntry(Map<Integer, V> attributes) {
        super(attributes);
    }

    @Override
    public V getValue(int i) {
        return this.attributes.get(i);
    }

    @Override
    public SVMEntry set(Map<Integer, V> attributes) {
        //this.index = index;
        //this.value = value;
        this.attributes = attributes;
        return this;
    }

}
