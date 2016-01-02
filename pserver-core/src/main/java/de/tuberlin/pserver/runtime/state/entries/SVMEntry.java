package de.tuberlin.pserver.runtime.state.entries;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by hegemon on 01.01.16.
 */
public abstract class SVMEntry<V extends Number> implements Entry<V> {

    //protected int index;
    //protected V value;
    protected Map<Integer, V> attributes;

    public SVMEntry(Map<Integer, V> attributes) {
        //this.index = index;
        //this.value = value;
        this.attributes = attributes;
    }

    abstract public V getValue(int i);

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator it = this.attributes.entrySet().iterator();
        sb.append("( ");
        while(it.hasNext()) {
            Map.Entry<Integer, V> entry = (Map.Entry) it.next();
            sb.append(entry.getKey() + ": " + entry.getValue());
            if (it.hasNext()) sb.append(", ");
            else sb.append(" )\n");
        }
        return sb.toString();
    }

}
