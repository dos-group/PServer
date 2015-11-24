package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.Cemetery;
import de.tuberlin.pserver.radt.hashtable.HashTable;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public abstract class AbstractLinkedList<T> extends AbstractRADT<T> implements ILinkedList<T> {
    // TODO: change to private
    protected final HashTable<S4Vector, Node<T>> svi; // S4Vector Index
    private Node<T> head;
    protected final Cemetery<Node<T>> cemetery;


    protected AbstractLinkedList(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        this.svi = new HashTable<>(id+"_svi", noOfReplicas, programContext);
        this.head = null;
        this.cemetery = new Cemetery<>();
    }

    public Node<T> getHead() {
        return head;
    }

    protected Node<T> getSVIEntry(S4Vector s4) {
        return svi.read(s4);
    }

    protected void setSVIEntry(Node<T> node) {
        svi.put(node.getS4HashKey(), node);
    }

    protected void setHead(Node<T> node) {
        this.head = node;
    }


}
