package de.tuberlin.pserver.dsl.controlflow.unit;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.base.Body;
import de.tuberlin.pserver.dsl.controlflow.base.CFStatement;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedVar;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.SlotContext;

import java.util.concurrent.CyclicBarrier;

public final class SlotParallelUnit extends CFStatement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ExecutionManager executionManager;

    private int fromSlotID;

    private int toSlotID;

    private CyclicBarrier slotGroupBarrier;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SlotParallelUnit(final SlotContext ic) {
        super(ic);
        executionManager = ic.runtimeContext.executionManager;
        allSlots();
    }

    // ---------------------------------------------------
    // Slot Selection.
    // ---------------------------------------------------

    public SlotParallelUnit slot(final int fromSlotID, final int toSlotID) {
        this.fromSlotID = fromSlotID;
        this.toSlotID = toSlotID;
        return this;
    }

    public SlotParallelUnit slot(final int slotID) { return slot(slotID, slotID); }

    public SlotParallelUnit allSlots() { return slot(0, slotContext.programContext.perNodeDOP - 1); }

    // ---------------------------------------------------
    // Synchronization.
    // ---------------------------------------------------

    public SlotParallelUnit sync() throws Exception {
        if (slotGroupBarrier != null)
            slotGroupBarrier.await();
        return this;
    }

    // ---------------------------------------------------
    // Execution.
    // ---------------------------------------------------

    public void exe(final Body body) throws Exception {
        Preconditions.checkNotNull(body);

        if (inSlotRange()) {

            final ExecutionManager.SlotGroupAllocation sa = executionManager.allocSlots(fromSlotID, toSlotID);

            final int numActiveSlots = (toSlotID - fromSlotID) + 1;

            if (numActiveSlots > 1) {

                final SharedVar<CyclicBarrier> sgb = new SharedVar<>(slotContext, new CyclicBarrier(numActiveSlots));

                slotGroupBarrier = sgb.get();

                sgb.done();
            }

            sync();

                enter();

                    long duration = System.currentTimeMillis();

                        body.body();

                    duration = System.currentTimeMillis() - duration;

                    setProfilingData(new ProfilingData(duration));

                leave();

            sync();

            executionManager.releaseSlots(sa);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private boolean inSlotRange() { return slotContext.slotID >= fromSlotID && slotContext.slotID <= toSlotID; }
}
