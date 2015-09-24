package de.tuberlin.pserver.mcruntime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.NestedIntervalTree;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class SlotGroup {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int minSlotID;

    public final int maxSlotID;

    public final NestedIntervalTree.Interval slotIDRange;

    private final CyclicBarrier barrier;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SlotGroup(final int minSlotID, final int maxSlotID) {
        this(new NestedIntervalTree.Interval(minSlotID, maxSlotID));
    }

    public SlotGroup(final NestedIntervalTree.Interval interval) {
        Preconditions.checkNotNull(interval);
        this.minSlotID   = interval.low;
        this.maxSlotID   = interval.high;
        this.slotIDRange = interval;
        this.barrier     = new CyclicBarrier(maxSlotID - minSlotID + 1);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public boolean contains(final int slotID) { return slotID >= minSlotID && slotID <= maxSlotID; }

    public int size() { return maxSlotID - minSlotID + 1; }

    public NestedIntervalTree.Interval asInterval() { return slotIDRange; }

    public void barrier() throws Exception {

        if (size() == 1) {
            return;
        }

        try {

            barrier.await(40000, TimeUnit.MILLISECONDS);

        } catch (TimeoutException | BrokenBarrierException e) {

            System.out.println(e + " -- " + this.toString());

            //MCRuntime.INSTANCE.printSlotStackTraces();

            /*System.out.println("getNumberWaiting() = " + barrier.getNumberWaiting()
                    + " | barrier.getParties() = " + barrier.getParties()
                    + " | " + this.toString()
                    + " | " + "[" + minSlotID + ", " + maxSlotID + "]"
                    + " | " + Thread.currentThread().getName()
                    + " | " + e.getClass().getSimpleName());*/

            throw e;
        }
    }

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

    // @Override  public String toString() { return "[" + minSlotID + ", " + maxSlotID + "]"; }
}