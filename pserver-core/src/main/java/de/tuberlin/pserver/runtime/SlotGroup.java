package de.tuberlin.pserver.runtime;


public final class SlotGroup {

    public final int minSlotID;

    public final int maxSlotID;

    public SlotGroup(final int minSlotID, final int maxSlotID) {

        this.minSlotID = minSlotID;

        this.maxSlotID = maxSlotID;
    }

    @Override
    public String toString() { return "[" + minSlotID + ", " + maxSlotID + "]"; }
}
