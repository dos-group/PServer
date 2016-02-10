package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.AbstractRADT;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.radt.hashtable.HashTableCemetery;
import de.tuberlin.pserver.radt.hashtable.HashTable;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractLinkedList<T> extends AbstractRADT<T> implements ILinkedList<T> {
    protected final Map<S4Vector, Node<T>> svi; // S4Vector Index
    private Node<T> head;
    protected final LinkedListCemetery<T> cemetery;


    protected AbstractLinkedList(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        this.svi = Collections.synchronizedMap(new HashMap<>());
        this.head = null;
        this.cemetery = new LinkedListCemetery<>(noOfReplicas, nodeId);
    }

    public Node<T> getHead() {
        return head;
    }

    protected Node<T> getSVIEntry(S4Vector s4) {
        return svi.get(s4);
    }

    protected void setSVIEntry(Node<T> node) {
        svi.put(node.getS4Vector(), node);
    }

    protected void setHead(Node<T> node) {
        this.head = node;
    }


}
