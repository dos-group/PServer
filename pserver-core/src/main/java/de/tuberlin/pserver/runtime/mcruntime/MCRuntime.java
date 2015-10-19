package de.tuberlin.pserver.runtime.mcruntime;

import de.tuberlin.pserver.commons.ds.NestedIntervalTree;
import de.tuberlin.pserver.core.common.Deactivatable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum MCRuntime implements Deactivatable {
    INSTANCE(Runtime.getRuntime().availableProcessors());

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Map<Long, WorkerSlot> threadToWorkerSlotMap;

    private final WorkerSlot[] workerSlots;

    private final Map<String, Object> sharedObjectMap;

    private NestedIntervalTree<SlotGroup> slotAssignment;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    private MCRuntime(final int numSlots) {

        this.threadToWorkerSlotMap = new ConcurrentHashMap<>();

        this.workerSlots = new WorkerSlot[numSlots];

        this.sharedObjectMap = new ConcurrentHashMap<>();

        clearAllocations();

        WorkerSlot.setMCRuntime(this);

        Parallel.setMCRuntime(this);

        create(numSlots);
    }

    @Override
    public void deactivate() {

        for (WorkerSlot workerSlot : workerSlots) {

            workerSlot.shutdown();
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int getNumOfWorkerSlots() { return workerSlots.length; }

    public WorkerSlot[] getWorkerSlots() { return workerSlots; }

    // ---------------------------------------------------

    public <T> void putSharedObject(final String name, final T obj) { sharedObjectMap.put(name, obj); }

    public <T> T removeSharedObject(final String name) { return (T)sharedObjectMap.remove(name); }

    public <T> T getSharedObject(final String name) { return (T)sharedObjectMap.get(name); }

    // ---------------------------------------------------

    public void registerWorkerSlot(final WorkerSlot workerSlot) {

        threadToWorkerSlotMap.put(Thread.currentThread().getId(), workerSlot);
    }

    public WorkerSlot currentSlot() {

        WorkerSlot ws = threadToWorkerSlotMap.get(Thread.currentThread().getId());

        while (ws == null) {
            ws = threadToWorkerSlotMap.get(Thread.currentThread().getId());
        }

        return ws;
    }

    public int currentSlotID() { return currentSlot().slotID; }

    // ---------------------------------------------------

    public synchronized SlotGroup currentSlotGroup() {

        final WorkerSlot ws = currentSlot();

        return slotAssignment.get(new NestedIntervalTree.Point(ws.slotID)).getRight();
    }

    public synchronized SlotGroup currentSlotGroup(final int slotID) {

        return slotAssignment.get(new NestedIntervalTree.Point(slotID)).getRight();
    }

    public synchronized SlotGroup allocSlots(final int dop) {

        final SlotGroup sg = currentSlotGroup();

        return allocSlots(sg.minSlotID, sg.minSlotID + dop - 1);
    }

    public synchronized SlotGroup allocSlots(final int low, final int high) {

        assert low == high;

        final NestedIntervalTree.Interval slotRange = new NestedIntervalTree.Interval(low, high);

        if (!slotAssignment.exist(slotRange)) {

            if (!slotAssignment.isValid(slotRange)) {
                throw new IllegalStateException("(1) Not a valid slot slotRange.\n" + slotAssignment.toString() + " ==>> " + slotRange);
            }

            final SlotGroup sg = new SlotGroup(slotRange);

            if (!slotAssignment.put(slotRange, sg))
                throw new IllegalStateException();

            return sg;

        } else {

            final SlotGroup currentSlotGroup = currentSlotGroup();

            // TODO: Check also intersection between currentSlotGroup and slotRange ?
            if (!currentSlotGroup.asInterval().contains(slotRange)) {
                throw new IllegalStateException("(2) Not a valid slot slotRange.\n" + slotAssignment.toString() + " ==>> " + slotRange);
            }

            return slotAssignment.get(slotRange).getRight();
        }
    }

    public void clearAllocations() {

        SlotGroup initialSlotGroup = new SlotGroup(0, workerSlots.length - 1);

        this.slotAssignment = new NestedIntervalTree<>(initialSlotGroup.asInterval(), initialSlotGroup);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    public MCRuntime create(final int numSlots) {

        for (int i = 0; i < numSlots; ++i) {

            workerSlots[i] = new WorkerSlot(i);

            if (i == 0) threadToWorkerSlotMap.put(Thread.currentThread().getId(), workerSlots[i]);
        }

        return this;
    }

    // ---------------------------------------------------
    // Debugging.
    // ---------------------------------------------------

    public synchronized String getSlotJavaStackTraces() {
        final  StringBuilder sb = new StringBuilder();
        for (int i = 0; i < workerSlots.length; ++i) {
            sb.append("-- ")
                    .append(workerSlots[i].getThreadName())
                    .append(" --")
                    .append('\n')
                    .append(workerSlots[i].getJavaStackTrace());
        }
        return sb.toString();
    }
}
