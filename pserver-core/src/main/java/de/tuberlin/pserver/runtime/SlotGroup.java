package de.tuberlin.pserver.runtime;


import de.tuberlin.pserver.commons.ds.NestedIntervalTree;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedVar;

import java.util.concurrent.CyclicBarrier;

public final class SlotGroup {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int minSlotID;

    public final int maxSlotID;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SlotGroup(final int minSlotID, final int maxSlotID) {
        this.minSlotID = minSlotID;
        this.maxSlotID = maxSlotID;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void sync(final SlotContext sc) throws Exception {
        final SharedVar<CyclicBarrier> slotGroupBarrier = new SharedVar<>(sc, new CyclicBarrier(size()));
        slotGroupBarrier.get().await();
        slotGroupBarrier.done();
    }

    public int size() { return maxSlotID - minSlotID + 1; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SlotGroup slotGroup = (SlotGroup) o;
        return maxSlotID == slotGroup.maxSlotID && minSlotID == slotGroup.minSlotID;
    }

    @Override
    public int hashCode() {
        int result = minSlotID;
        result = 31 * result + maxSlotID;
        return result;
    }

    @Override
    public String toString() { return "[" + minSlotID + ", " + maxSlotID + "]"; }

    public NestedIntervalTree.Interval asInterval() {
        return new NestedIntervalTree.Interval(minSlotID, maxSlotID);
    }
}
