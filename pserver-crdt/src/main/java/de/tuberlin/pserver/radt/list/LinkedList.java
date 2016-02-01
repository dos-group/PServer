package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.radt.S4Vector;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public class LinkedList<T> extends AbstractLinkedList<T>{

    // TODO: do I need size?
    public LinkedList(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
    }

    public boolean insert(int index, T value) {
        Node<T> node;
        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(sessionID, nodeId, clock, 0);

        node = localInsert(index, value, s4, clock);

        if(node != null) {
            broadcast(new ListOperation<>(Operation.OpType.INSERT, node, node.getRefNodeS4(), clock, s4));
            return true;
        }

        return false;
        /*System.out.println("Local put " + value + " at " + siteID + " with Vectorclock: <" + s4.getSessionNumber() +
                ", "+ s4.getSiteId() + ", " + s4.getVectorClockSum() + ", " + s4.getSeq() +">");
        System.out.println(this);*/
    }

    public boolean update(int index, T value) {
        Node<T> node;
        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(sessionID, nodeId, clock, 0);

        node = localUpdate(index, value);

        if(node != null) {
            broadcast(new ListOperation<>(Operation.OpType.UPDATE, node, node.getRefNodeS4(), clock, s4));
            return true;
        }

        return false;
    }

    public boolean delete(int index) {
        int[] clock = increaseVectorClock();

        S4Vector s4 = new S4Vector(sessionID, nodeId, clock, 0);

        Node<T> node = localDelete(index);

        if(node != null) {
            // refNode can't be null because that means inserting at the head
            broadcast(new ListOperation<>(Operation.OpType.DELETE, node, null, clock, s4));
            return true;
        }

        return false;
    }

    @Override
    public T read(int i) {
        // TODO: implement this!
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LinkedList{\n");
        Node<T> node = getHead();
        while(node != null) {
            sb.append(node.getValue() + "\n");
            node = node.getLink();
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        ListOperation<Node<T>> listOp = (ListOperation<Node<T>>) op;

        if(listOp.getType() == Operation.OpType.INSERT) {
            // System.out.println("Received PUT: " + listOp.getValue());
            /*System.out.println("Received Key: " + listOp.getRefS4Vector() + ", Value: " + listOp.getValue());
            System.out.println(svi);
            System.out.println(this);
            System.out.println(getSVIEntry(listOp.getRefS4Vector()));*/
            // TODO: causally ready etc. !
            return remoteInsert(listOp.getValue(), listOp.getRefS4Vector());
        }
        else if(listOp.getType() == Operation.OpType.UPDATE) {
            return remoteUpdate(listOp.getValue());
        }
        else if(listOp.getType() == Operation.OpType.DELETE) {
            return remoteDelete(listOp.getValue().getS4HashKey());
        }
        else {
            // TODO: exception message
            throw new UnsupportedOperationException("blub");
        }
        
    }

    private Node<T> localInsert(int index, T value, S4Vector s4, int[] vectorClock) {
        Node<T> node;

        Node<T> ref = getNodeByIndex(index);
        if(ref != null) {
            // TODO: this is probably wrong => see Roh et al (doesn't make sense) to try and get it right
            node = new Node<>(value, s4, s4, ref.getNext(), ref.getLink(), ref.getS4HashKey(), vectorClock);

            setSVIEntry(node);
            ref.setNext(node);
            ref.setLink(node);

            return node;
        }
        else if(getHead() == null && index == 0) {
            node = new Node<>(value, s4, s4, null, null, null, vectorClock);

            setSVIEntry(node);
            setHead(node);

            return node;
        }
        else {
            return null;
        }
    }

    private Node<T> localUpdate(int i, T value) {
        Node<T> node = getNodeByIndex(i);
        if(node != null) node.setValue(value);

        return node;
    }

    private Node<T> localDelete(int index) {
        Node<T> node = getNodeByIndex(index);
        if(node != null) node.makeTombstone();;

        return node;
    }

    private boolean remoteInsert(Node<T> node, S4Vector refS4) {
        Node<T> ref = null; // The node to the left of where we want to insert

       /* if(refS4 == null && getHead() == null) {
            node.setLink(null);
            node.setNext(null);
            setHead(node);

            return true;
        }*/

        // 1. Find the left node in the hash table
        if(refS4 != null) {
            ref = getSVIEntry(refS4);

            while(ref != null && !ref.getS4HashKey().equals(refS4)) {
                System.out.println("***");
                ref = ref.getNext();
            }

            if(ref == null) {
                // TODO: throw new NoRefObjException
                throw new RuntimeException("It's Over !!!!!");}
        }

        node.setNext(getSVIEntry(node.getS4HashKey()));
        // TODO: is this necessary? What is exactly going on here?
        setSVIEntry(node);

        // 2. Make new node
        //node = new Node<>(value, s4, s4, )

        // 3. Scan possible places to insert the node
        if(refS4 == null) {
            if(getHead() == null || getHead().getS4HashKey().takesPrecedenceOver(node.getS4HashKey())) {
                if(getHead() != null) {
                    node.setLink(getHead());

                } else {
                    setHead(node);
                    return true;
                }
                // Do I need to create a node here first?

                setHead(node);
                //setSVIEntry(node);
                return true;
            }
            else {
                ref = getHead();
            }
        }

        while(ref.getLink() != null && node.getS4HashKey().takesPrecedenceOver(ref.getLink().getS4HashKey())) {
            ref = ref.getLink();
        }

        node.setLink(ref.getLink());
        ref.setLink(node);
        return true;
    }

    private boolean remoteUpdate(Node<T> node) {
        Node<T> oldNode = getSVIEntry(node.getS4HashKey());

        System.out.println("***"+oldNode);
        System.out.println(oldNode.getS4HashKey());
        System.out.println(node);
        System.out.println(node.getS4HashKey());

        while(oldNode != null && !oldNode.getS4HashKey().equals(node.getS4HashKey())) {
            oldNode = oldNode.getNext();
        }

        if(oldNode == null) {
            // TODO: throw new NoTargetObjException
            throw new RuntimeException("That's all folks!");
        }

        if(oldNode.isTombstone()) return false;
        if(node.getS4Vector().takesPrecedenceOver(oldNode.getS4Vector())) return false;


        oldNode.setValue(node.getValue());
        oldNode.setS4Vector(node.getS4Vector());
        return true;
    }

    private boolean remoteDelete(S4Vector s4) {
        Node<T> node = getSVIEntry(s4);

        while(node != null && !node.getS4HashKey().equals(s4)) {
            node = node.getNext();
        }

        if(node == null) {
            // TODO: throw new NoTargetObjException
            throw new RuntimeException("Oh no!");
        }

        if(!node.isTombstone()) {
            node.makeTombstone();
            node.setS4Vector(s4);
            cemetery.enrol(node);
        }

        return true;
    }



    private Node<T> getNodeByIndex(int index) {
        Node<T> node = getHead();
        if(index == 0) return node;

        int k = 0;

        while(node != null) {
            if(!node.isTombstone()) {
                // The first object is referred to by index 1. An insert adds a new node next to (on the right of) its
                // reference. To insert x at the head, we use Insert(0,x).
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
