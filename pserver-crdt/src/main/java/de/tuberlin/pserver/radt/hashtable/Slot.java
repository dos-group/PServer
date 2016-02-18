package de.tuberlin.pserver.radt.hashtable;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Slot<K,V> extends CObject<V> {
    private final K key;

    // no-arg constructor for serialization
    public Slot() {
        this.key = null;
    }

    public Slot(K key, V value, S4Vector s4) {
        super(s4, value);
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        final Slot<?, ?> other = (Slot<?, ?>) obj;

        return new EqualsBuilder()
                .appendSuper(super.equals(other))
                .append(this.key, other.key)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(key)
                .toHashCode();
    }
}
