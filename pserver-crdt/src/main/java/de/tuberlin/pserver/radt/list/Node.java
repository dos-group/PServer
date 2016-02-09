package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;

import java.io.Serializable;

public class Node<T> extends CObject<T> implements Serializable {
    private final S4Vector s4HashKey;
    private final S4Vector refNodeS4;
    private Node<T> next; // Next in hash table
    private Node<T> link; // Next in linked list

    // no-args constructor for serialization
    public Node() {
        this.s4HashKey = null;
        this.refNodeS4 = null;
        this.next = null;
        this.link = null;
    }

    public Node(T value, S4Vector s4HashKey, S4Vector s4Vector, Node<T> next, Node<T> link, S4Vector refNodeS4) {
        super(s4Vector, value);
        this.s4HashKey = s4HashKey;
        this.refNodeS4 = refNodeS4;
        this.next = next;
        this.link = link;
    }

    public void setNext(Node<T> next) {
        this.next = next;
    }

    public void setLink(Node<T> link) {
        this.link = link;
    }

    public S4Vector getS4HashKey() {
        return s4HashKey;
    }

    public Node<T> getNext() {
        return next;
    }

    public Node<T> getLink() {
        return link;
    }

    public boolean isTombstone() {
        return getValue() == null;
    }

    public void makeTombstone() {
        this.setValue(null);
    }

    public S4Vector getRefNodeS4() {
        return refNodeS4;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getValue().toString());
        return sb.toString();
    }
}
