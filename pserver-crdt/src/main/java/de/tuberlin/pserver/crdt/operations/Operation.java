package de.tuberlin.pserver.crdt.operations;

import java.io.Serializable;

public interface Operation<T> extends Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /** The end token for CRDT operations. Sending this token signals that a replica is finished sending updates.*/
    int END = -1;

    /** Increment token for CRDT operations. (i.e. "a + b") */
    int INCREMENT = 1;

    /** Decrement token for CRDT operations. (i.e. "a - b") */
    int DECREMENT = 2;

    /** Add token for CRDT operations. (i.e. "add this element to a replica)" */
    int ADD = 3;

    /** Remove token for CRDT operations. (i.e. "remove this element from a replica") */
    int REMOVE = 4;

    /** Write token for CRDT operations. (i.e. "write to register") */
    int WRITE = 5;

    /** Put token for CRDT operation. (i.e. "put to hashtable") */
    int PUT = 6;

    /** Insert token for RADT operation **/
    int INSERT = 7;

    /** Insert token for RADT operation **/
    int UPDATE = 8;

    /** Insert token for RADT operation **/
    int DELETE = 9;



    default String getOperationType() {
        switch(getType()) {
            case END:       return "END";
            case INCREMENT: return "INCREMENT";
            case DECREMENT: return "DECREMENT";
            case ADD:       return "ADD";
            case REMOVE:    return "REMOVE";
            case WRITE:     return "WRITE";
            case PUT:       return "PUT";
            case INSERT:    return "INSERT";
            case UPDATE:    return "UPDATE";
            case DELETE:    return "DELETE";
            default:        return "UNKNOWN";
        }
    }

    int getType();
    T getValue();
}