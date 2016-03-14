package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.CObject;

import java.io.Serializable;

public class Node<T> extends CObject<T> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // Regular s4 vector for hash key and precedence of inserts is in super class
    private S4Vector updateDeleteS4; // For precedence of deletes and updates

    private Node<T> next; // Next in hash table

    private Node<T> link; // Next in linked list

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // no-args constructor for serialization
    public Node() {

        this.updateDeleteS4 = null;

        this.link = null;

    }

    public Node(T value, S4Vector s4Vector, S4Vector updateDeleteS4, Node<T> link) {

        super(s4Vector, value);

        this.updateDeleteS4 = updateDeleteS4;

        this.link = link;

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public synchronized void setLink(Node<T> link) {

        this.link = link;

    }

    public synchronized Node<T> getLink() {

        return link;

    }

    public synchronized S4Vector getUpdateDeleteS4() {

        return updateDeleteS4;

    }

    public synchronized void setUpdateDeleteS4(S4Vector s4) {

        this.updateDeleteS4 = s4;

    }

    @Override
    public synchronized String toString() {

        final StringBuilder sb = new StringBuilder(getValue().toString());

        return sb.toString();

    }

}
