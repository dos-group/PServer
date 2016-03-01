package de.tuberlin.pserver.radt;


import de.tuberlin.pserver.operations.SimpleOperation;

public abstract class RADTOperation<V> extends SimpleOperation<V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int[] vectorClock;
    private final S4Vector s4;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // No arg constructor for serialization
   public RADTOperation() {
        super();
        this.vectorClock = null;
        this.s4 = null;
    }

    public RADTOperation(OpType type, V value, int[] vectorClock, S4Vector s4) {
        super(type, value);
        this.vectorClock = vectorClock;
        this.s4 = s4;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int[] getVectorClock() {
        return vectorClock;
    }

    public S4Vector getS4Vector() {
        return s4;
    }
}
