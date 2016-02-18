package de.tuberlin.pserver.crdt.operations;

import java.io.Serializable;

public interface Operation<T> extends Serializable {

    // TODO: Reduce the number of OpTypes to a useful amount
    enum OpType {
        /** The end token for CRDT operations. Sending this token signals that a replica is finished sending updates.*/
        END,
        /** Increment token for CRDT operations. (i.e. "a + b") */
        INCREMENT,
        /** Decrement token for CRDT operations. (i.e. "a - b") */
        DECREMENT,
        /** Add token for CRDT operations. (i.e. "add this element to a replica)" */
        ADD,
        /** Remove token for CRDT operations. (i.e. "remove this element from a replica") */
        REMOVE,
        /** Assign token for CRDT operations. (i.e. "assign to register") */
        ASSIGN,
        /** Put token for CRDT operation. (i.e. "put to hashtable") */
        PUT,
        /** Write token for RADT operation (i.e. "write to array") */
        WRITE,
         /** Insert token for RADT operation **/
        INSERT,
        /** Update token for RADT operation **/
        UPDATE,
        /** Delete token for RADT operation **/
        DELETE,
        /** Set token for RADT operation **/
        SET,
    }

    OpType getType();
    T getValue();
}