package de.tuberlin.pserver.radt.list;

import com.clearspring.analytics.util.Preconditions;
import de.tuberlin.pserver.crdt.exceptions.CRDTException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.ArrayList;
import java.util.List;

public class LinkedList<T> extends AbstractLinkedList<T> {

    // TODO: do I need size?
    public LinkedList(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        ready();
    }

    private synchronized boolean insertAtHead(T value) {
        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);

        Node<T> node = new Node<>(value, s4, s4, getHead());

        svi.put(s4, node);
        setHead(node);

        broadcast(new ListOperation<>(Operation.OpType.INSERT, value, null, clock, s4));

        return true;
    }

    // From Roh et al: The first object is referred to by index 1. An insert adds a new node next to (on the right of)
    // its reference. To insert x at the head, we use Insert(0,x).
    @Override
    public synchronized boolean insert(int index, T value) {
        if(index == 0) return insertAtHead(value);

        Node<T> refNode = getNodeByIndex(index);

        if (refNode == null) return false;

        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);

        // TODO: does the node constructor need two fields for s4 or is it always the same value?
        Node<T> node = new Node<>(value, s4, s4, refNode.getLink());

        svi.put(s4, node);
        refNode.setLink(node);

        broadcast(new ListOperation<>(Operation.OpType.INSERT, node.getValue(), refNode.getS4Vector(), clock, s4));


        return true;

    }

    @Override
    public T read(int i) {
        Node<T> node = getNodeByIndex(i);

        return node != null ? node.getValue() : null;
    }

    @Override
    public synchronized boolean update(int index, T value) {
        index++; // to adjust for getNodeById() method returning element left of index (for inserts etc.)
        // TODO: error text
        // TODO: index out of bounds check?
        Preconditions.checkArgument(value != null);
        Node<T> node = getNodeByIndex(index);

        if(node == null) return false;

        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);
        node.setUpdateDeleteS4(s4);

        node.setValue(value);

        // TODO: reorder listoperation arguments
        broadcast(new ListOperation<>(Operation.OpType.UPDATE, value, s4, clock, node.getS4Vector()));

        return true;
    }

    @Override
    public synchronized boolean delete(int index) {
        index++; // to adjust for getNodeById() method returning element left of index (for inserts etc.)
        Node<T> node = getNodeByIndex(index);

        if(node == null) return false;

        System.out.println("Found node " + node.getValue());


        cemetery.enrol(nodeId, node);

        int[] clock = increaseVectorClock();
        S4Vector s4 = new S4Vector(nodeId, clock);
        node.setUpdateDeleteS4(s4);

        broadcast(new ListOperation<>(Operation.OpType.DELETE, null, node.getUpdateDeleteS4(), clock, node.getS4Vector()));

        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("\nLinkedList {");
        Node<T> node = getHead();

        while(node != null) {
            if(!node.isTombstone()) sb.append(node.getValue());
            else sb.append("tombstone");

            if(node.getLink() != null) sb.append(", ");
            else sb.append("}\n");
            node = node.getLink();
        }
        return sb.toString();
    }


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
    public List<T> getList() {
        ArrayList<T> list = new ArrayList<>();

        Node<T> node = getHead();
        while(node != null && !node.isTombstone()) {
            list.add(node.getValue());
            node = node.getLink();
        }

    return list;
    }

    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        @SuppressWarnings("unchecked")
        ListOperation<T> listOp = (ListOperation<T>) op;

        switch(listOp.getType()) {
            case INSERT:
                return remoteInsert(listOp.getValue(), listOp.getS4Vector(), listOp.getSecondaryS4());
            case UPDATE:
                return remoteUpdate(listOp.getS4Vector(), listOp.getValue(), listOp.getSecondaryS4());
            case DELETE:
                return remoteDelete(listOp.getS4Vector(), listOp.getSecondaryS4(), srcNodeId);
            default:
                throw new IllegalArgumentException("LinkedList RADTs do not allow the " + op.getType() + " operation.");
        }
    }

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

        return true;
    }

    private synchronized boolean remoteInsert(T value, S4Vector s4, S4Vector refS4) {
        //System.out.println("\n[" + nodeId +"] Inserting value: " + value + " s4: " + s4 + " refS4: " + refS4);
        //System.out.println(this + "\n");

        if (refS4 == null) return remoteInsertAtHead(value, s4);

        // 1. Find the left node (reference node) in the hash table
        Node<T> refNode = getSVIEntry(refS4);

        if (refNode == null)
            throw new CRDTException("Could not find the reference node " + refS4 + " for remote insert " + value);


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

        return true;
    }

    private boolean remoteUpdate(S4Vector currS4, T value, S4Vector newS4) {
        Node<T> node = getSVIEntry(currS4);

        if(node == null) throw new CRDTException("Could not find the node " + currS4 + " for remote update with " + value);

        if(node.isTombstone()) return false;

        if(newS4.precedes(node.getUpdateDeleteS4())) return false;

        node.setValue(value);
        node.setUpdateDeleteS4(newS4);

        return true;
    }

    private boolean remoteDelete(S4Vector s4, S4Vector newUpadteDeleteS4, int srcNodeId) {
        Node<T> node = getSVIEntry(s4);

        if(node == null) throw new CRDTException("Could not find the node " + s4 + " for deletion");

        if(node.isTombstone()) return true;

        node.setUpdateDeleteS4(newUpadteDeleteS4);
        cemetery.enrol(srcNodeId, node);

        return true;
    }


    // From Roh et al: The first object is referred to by index 1. An insert adds a new node next to (on the right of)
    // its reference. To insert x at the head, we use Insert(0,x).
    private Node<T> getNodeByIndex(int index) {
        Node<T> node = getHead();
        int k = 0;

        while(node != null) {
            if(!node.isTombstone()) {
                if(index == ++k) {
                    return node;
                } else {
                    node = node.getLink();
                }
            }
        }

        return null;
    }
}
