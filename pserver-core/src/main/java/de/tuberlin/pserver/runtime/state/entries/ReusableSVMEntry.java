package de.tuberlin.pserver.runtime.state.entries;

import java.util.Map;

/**
 * Created by hegemon on 01.01.16.
 */
public interface ReusableSVMEntry<V extends Number> extends ReusableEntry<V> {

    Entry set(Map<Integer, V> attributes);

}
