package de.tuberlin.pserver.radt.list;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.ReplicatedDataTypeException;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.ArrayList;
import java.util.List;


public class LinkedList<T> extends AbstractLinkedList<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final LinkedListCemetery<T> cemetery;

    private final Object listLock = new Object();

    private int size;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LinkedList(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        this.cemetery = new LinkedListCemetery<>(this, noOfReplicas, nodeId);

        this.size = 0;

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    // From Roh et al: The first object is referred to by index 1. An insert adds a new node next to (on the right of)
    // its reference. To insert x at the head, we use Insert(0,x).
    @Override
    public synchronized boolean insert(int index, T value) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        if(index == 0) return insertAtHead(value);

        Node<T> refNode = getNodeByIndex(index);

        if (refNode == null) return false;

        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(nodeId, clock);

        Node<T> node = new Node<>(value, s4, s4, refNode.getLink());

        svi.put(s4, node);

        refNode.setLink(node);

        size++;

        broadcast(new ListOperation<>(Operation.OpType.INSERT, node.getValue(), s4, refNode.getS4Vector(), clock));

        return true;

    }

    @Override
    public T read(int i) {

        Node<T> node = getNodeByIndex(i);

        return node != null ? node.getValue() : null;

    }

    @Override
    public synchronized boolean update(int index, T value) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        index++; // to adjust for getNodeById() method returning element left of index (for inserts etc.)

        Preconditions.checkArgument(value != null);

        Node<T> node = getNodeByIndex(index);

        if(node == null) return false;

        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(nodeId, clock);

        node.setUpdateDeleteS4(s4);

        node.setValue(value);

        broadcast(new ListOperation<>(Operation.OpType.UPDATE, value, s4, node.getS4Vector(), clock));

        return true;

    }

    @Override
    public synchronized boolean delete(int index) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        index++; // to adjust for getNodeById() method returning element left of index (for inserts etc.)

        Node<T> node = getNodeByIndex(index);

        if(node == null) return false;

        cemetery.enrol(nodeId, node);

        size--;

        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(nodeId, clock);

        node.setUpdateDeleteS4(s4);

        broadcast(new ListOperation<>(Operation.OpType.DELETE, null, s4, node.getS4Vector(), clock));

        return true;
    }

    public synchronized int getTombstones() {

        int sum = 0;

        Node<T> node = getHead();

        while(node != null) {

            if(node.isTombstone()) sum++;

            node = node.getLink();

        }

        return sum;

    }

    /*@Override
    public synchronized String toString() {
        final StringBuilder sb = new StringBuilder("\nLinkedList {");
        Node<T> node = getHead();

        while(node != null) {
            if(!node.isTombstone()) sb.append(node.getValue());
            else sb.append("t");

            if(node.getLink() != null) sb.append(", ");
            else sb.append("}\n");
            node = node.getLink();
        }
        return sb.toString();
    }*/



    //FOR DEBUG

    /*@Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("\nLinkedList {\n");
        Node<T> node = getHead();

        while(node != null) {
            if(!node.isTombstone()) sb.append(node.getValue());
            else sb.append("tombstone");

            sb.append("   | " + node.getS4Vector() + " | " + node.getUpdateDeleteS4());

            if(node.getLink() != null) sb.append(", \n");
            else sb.append("}\n");
            node = node.getLink();
        }
        return sb.toString();
    }*/


    @Override
    public synchronized List<T> getList() {

        ArrayList<T> list = new ArrayList<>();

        Node<T> node = getHead();

        while(node != null) {

            if(!node.isTombstone()) list.add(node.getValue());

            node = node.getLink();

        }

        return list;
    }

    public int size() {

        return size;

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected synchronized boolean update(int srcNodeId, Operation<?> op) {

        @SuppressWarnings("unchecked")
        ListOperation<T> listOp = (ListOperation<T>) op;

        boolean result;

        switch(listOp.getType()) {

            case INSERT:

                result = remoteInsert(listOp.getValue(), listOp.getS4Vector(), listOp.getRefS4());

                cemetery.updateAndPurge(vectorClock, listOp.getVectorClock(), srcNodeId);

                return result;

            case UPDATE:

                result = remoteUpdate(listOp.getValue(), listOp.getRefS4(), listOp.getS4Vector());

                cemetery.updateAndPurge(vectorClock, listOp.getVectorClock(), srcNodeId);

                return result;

            case DELETE:

                result = remoteDelete(listOp.getRefS4(), listOp.getS4Vector(), srcNodeId);

                cemetery.updateAndPurge(vectorClock, listOp.getVectorClock(), srcNodeId);

                return result;

            default:

                throw new IllegalArgumentException("LinkedList RADTs do not allow the " + op.getType() + " operation.");

        }

    }

    // ---------------------------------------------------
    // Package Private Methods.
    // ---------------------------------------------------

    /* package */ synchronized boolean removeTombstone(Node<T> node) {

        Preconditions.checkArgument(node != null);

        Preconditions.checkArgument(node.isTombstone());

        if(getHead().equals(node)) return removeTombstoneFromHead(node);

        Node<T> refNode = getHead();

        while(refNode != null && !node.equals(refNode.getLink())) {

            refNode = refNode.getLink();

        }

        if(refNode == null) throw new ReplicatedDataTypeException("Could not find the correct reference node");

        svi.remove(node.getS4Vector());

        refNode.setLink(node.getLink());

        node.setLink(null);

        return true;

    }

    // From Roh et al: The first object is referred to by index 1. An insert adds a new node next to (on the right of)
    // its reference. To insert x at the head, we use Insert(0,x).
    /* package */ synchronized Node<T> getNodeByIndex(int index) {

        Node<T> node = getHead();

        int k = 0;

        while(node != null) {

            if(!node.isTombstone()) {

                if(index == ++k) {

                    return node;

                }

            }

            node = node.getLink();

        }

        return null;

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized boolean remoteInsertAtHead(T value, S4Vector s4) {

        Node<T> node = new Node<>(value, s4, s4, null);

        svi.put(s4, node);

        if(getHead() == null) {

            setHead(node);

        }

        else if(getHead().getS4Vector().precedes(s4)) {

            node.setLink(getHead());

            setHead(node);

        }

        else {

            Node<T> refNode = getHead();

            // Find the right place to insert
            while(refNode.getLink() != null && s4.precedes(refNode.getLink().getS4Vector()))

                refNode = refNode.getLink();

            node.setLink(refNode.getLink());

            refNode.setLink(node);

        }

        size++;

        return true;

    }

    private synchronized boolean remoteInsert(T value, S4Vector s4, S4Vector refS4) {

        if (refS4 == null) return remoteInsertAtHead(value, s4);

        // 1. Find the left node (reference node) in the hash table
        Node<T> refNode = getSVIEntry(refS4);

        if (refNode == null)
            throw new ReplicatedDataTypeException("Node " + nodeId + " could not find the reference node " + refS4 + " for remote insert " + value);

        // 2. Make new node
        Node<T> node = new Node<>(value, s4, s4, null);

        // 3. Place new node into hashtable
        svi.put(s4, node);

        // 4. Find the right place to insert
        while(refNode.getLink() != null && node.getS4Vector().precedes(refNode.getLink().getS4Vector()))

            refNode = refNode.getLink();

        // 5. Insert into list
        node.setLink(refNode.getLink());

        refNode.setLink(node);

        size++;

        return true;
    }

    private synchronized boolean remoteUpdate(T value, S4Vector refS4, S4Vector s4) {

        Node<T> node = getSVIEntry(refS4);

        if(node == null) throw new ReplicatedDataTypeException("Node " + nodeId + " could not find the node " + refS4 + " for remote update with " + value);

        if(node.isTombstone()) return false;

        if(s4.precedes(node.getUpdateDeleteS4())) return false;

        node.setValue(value);

        node.setUpdateDeleteS4(s4);

        return true;

    }

    private synchronized boolean remoteDelete(S4Vector refS4, S4Vector s4, int srcNodeId) {

        Node<T> node = getSVIEntry(refS4);

        if(node == null) throw new ReplicatedDataTypeException("Node " + nodeId + " Could not find the node " + refS4 + " for deletion");

        if(node.isTombstone()) {

            return true;

        }

        node.setUpdateDeleteS4(s4);

        cemetery.enrol(srcNodeId, node);

        size--;

        return true;

    }

    private synchronized boolean removeTombstoneFromHead(Node<T> node) {

        svi.remove(node.getS4Vector());

        setHead(node.getLink());

        return true;

    }


    private synchronized boolean insertAtHead(T value) {

        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(nodeId, clock);

        Node<T> node = new Node<>(value, s4, s4, getHead());

        svi.put(s4, node);

        setHead(node);

        size++;

        broadcast(new ListOperation<>(Operation.OpType.INSERT, value, s4, null, clock));

        return true;

    }

}
