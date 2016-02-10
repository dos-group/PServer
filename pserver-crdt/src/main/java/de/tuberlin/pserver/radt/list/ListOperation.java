package de.tuberlin.pserver.radt.list;

import de.tuberlin.pserver.radt.RADTOperation;
import de.tuberlin.pserver.radt.S4Vector;

public class ListOperation<T> extends RADTOperation<T> {
    // S4 vector of the node to the left of the inserted node. Used in insert operations to maintain intention

    /* The secondary S4 vector can be different depending on which type of list operation is being executed:
     * INSERT: secondary s4 is the s4 vector of the node to the left of the node to be inserted
     * UPDATE: secondary s4 is the s4 vector used for precedence of updates/deletes (s_p in Roh et al.)
     * DELETE: secondary s4 is the s4 vector used for precedence of updates/deletes (s_p in Roh et al.)
     */
    // TODO: explain the different types
    private final S4Vector secondaryS4;

    // no-args constructor for serialization
    public ListOperation() {
        this.secondaryS4 = null;
    }

    public ListOperation(OpType type, T value, S4Vector secondaryS4, int[] vectorClock, S4Vector s4) {
        super(type, value, vectorClock, s4);
        this.secondaryS4 = secondaryS4;
    }

    public S4Vector getSecondaryS4() {
        return secondaryS4;
    }
}
