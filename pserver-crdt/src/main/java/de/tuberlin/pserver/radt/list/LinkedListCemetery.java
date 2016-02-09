package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.AbstractCemetery;
import de.tuberlin.pserver.radt.Cemetery;

import java.util.Hashtable;
// TODO: this is shit.
public class LinkedListCemetery<T> extends AbstractCemetery<Node<T>> implements Cemetery<Node<T>> {

    public LinkedListCemetery(int noOfReplicas,int nodeId) {
        super(noOfReplicas, nodeId);
    }

    @Override
    public boolean purge() {
        return false;
    }
}
