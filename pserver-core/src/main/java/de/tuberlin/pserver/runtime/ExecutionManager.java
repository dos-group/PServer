package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.NestedIntervalTree;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.dsl.controlflow.base.CFStatement;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;
import de.tuberlin.pserver.types.DistributedMatrix;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public final class ExecutionManager {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String BSP_SYNC_BARRIER_EVENT = "bsp_sync_barrier_event";

    public static enum CallType {

        SYNC,

        ASYNC
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class SlotGroupAllocation {

        public NestedIntervalTree.Interval slotIDRange;

        public ExecutionFrame[] frames;

        public SlotGroupAllocation(final NestedIntervalTree.Interval slotIDRange) {

            this.slotIDRange = Preconditions.checkNotNull(slotIDRange);

            this.frames = new ExecutionFrame[(slotIDRange.high - slotIDRange.low) + 1];
        }

        public int size() { return frames.length; }
    }

    // ---------------------------------------------------

    public static final class ExecutionFrame {

        public final ExecutionFrame parent;

        public final CFStatement statement;

        public final SlotContext slotContext;

        public final int frameLevel;

        public ExecutionFrame(final int frameLevel,
                              final ExecutionFrame parent,
                              final CFStatement statement,
                              final SlotContext slotContext) {

            this.frameLevel = frameLevel;

            this.parent = parent;

            this.statement = Preconditions.checkNotNull(statement);

            this.slotContext = Preconditions.checkNotNull(slotContext);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);

    public final int numOfCores;

    private final NetManager netManager;

    // Execution Context.

    private final AtomicReference<MLProgramContext> programContextRef;

    // DSL Runtime.

    private final ExecutionFrame[] activeFrame;

    private final ThreadLocal<MutableInt> currentFrameLevel;

    private NestedIntervalTree<SlotGroupAllocation> slotAssignment;

    private final Object lock = new Object();

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ExecutionManager(final int numOfCores, final NetManager netManager) {

        this.numOfCores = numOfCores;

        this.netManager = Preconditions.checkNotNull(netManager);

        this.programContextRef = new AtomicReference<>(null);

        this.netManager.addEventListener(BSP_SYNC_BARRIER_EVENT, event -> {

            if (!programContextRef.get().programID.equals(event.getPayload()))
                throw new IllegalStateException();

                programContextRef.get().countDownBarrier();
        });

        // DSL Runtime.

        this.activeFrame = new ExecutionFrame[numOfCores];

        this.currentFrameLevel = new ThreadLocal<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void registerProgram(final MLProgramContext programContext) {
        Preconditions.checkNotNull(programContext);

        final int activeSlots = programContext.perNodeDOP;

        programContextRef.set(programContext);

        final NestedIntervalTree.Interval in = new NestedIntervalTree.Interval(0, activeSlots - 1);

        final SlotGroupAllocation sa = new SlotGroupAllocation(in);

        this.slotAssignment = new NestedIntervalTree<>(in, sa);
    }

    public void unregisterProgram(final MLProgramContext programContext) {
        Preconditions.checkNotNull(programContext);

        programContextRef.set(null);
    }

    public synchronized SlotContext getSlotContext() {

        SlotContext sc = programContextRef.get().threadIDSlotCtxMap.get(Thread.currentThread().getId());

        while (sc == null)
            sc = programContextRef.get().threadIDSlotCtxMap.get(Thread.currentThread().getId());

        return sc;
    }

    // ---------------------------------------------------
    // DSL RUNTIME MANAGEMENT.
    // ---------------------------------------------------

    public synchronized void pushFrame(final CFStatement statement) {

        final SlotContext sc = statement.slotContext;

        if (currentFrameLevel.get() == null) {

            currentFrameLevel.set(new MutableInt(0));
        }

        currentFrameLevel.get().increment();

        final ExecutionFrame newFrame = new ExecutionFrame(
                currentFrameLevel.get().getValue(),
                activeFrame[statement.slotContext.slotID],
                statement,
                sc
        );

        activeFrame[sc.slotID] = newFrame;

        final Pair<NestedIntervalTree.Interval, SlotGroupAllocation> slotGroup = slotAssignment.get(new NestedIntervalTree.Point(sc.slotID));

        final int index = statement.slotContext.slotID - slotGroup.getLeft().low;

        slotGroup.getRight().frames[index] = newFrame;
    }

    public void popFrame() {

        final SlotContext sc = getSlotContext();

        activeFrame[sc.slotID] = activeFrame[sc.slotID].parent;

        currentFrameLevel.get().decrement();
    }

    public synchronized int getScopeDOP() {

        final SlotContext ic = getSlotContext();

        return slotAssignment.get(new NestedIntervalTree.Point(ic.slotID)).getRight().size();
    }

    public synchronized SlotGroup getActiveSlotGroup() {

        final SlotContext sc = getSlotContext();

        final NestedIntervalTree.Interval slots = slotAssignment.get(new NestedIntervalTree.Point(sc.slotID)).getLeft();

        return new SlotGroup(slots.low, slots.high);
    }

    // ---------------------------------------------------
    // SLOT ASSIGNMENT.
    // ---------------------------------------------------

    public synchronized SlotGroupAllocation allocSlots(final int low, final int high) {

        assert low == high;

        final NestedIntervalTree.Interval slotRange = new NestedIntervalTree.Interval(low, high);

        if (!slotAssignment.exist(slotRange)) {

            if (!slotAssignment.isValid(slotRange)) {
                throw new IllegalStateException("(1) Not a valid slot slotRange.\n" + slotAssignment.toString() + " ==>> " + slotRange);
            }

            final SlotGroupAllocation sa = new SlotGroupAllocation(slotRange);

            if (!slotAssignment.put(slotRange, sa))
                throw new IllegalStateException();

            return sa;

        } else {

            final SlotGroup currentSlotGroup = getSlotContext().getActiveSlotGroup();

            // TODO: Check also intersection between currentSlotGroup and slotRange ?
            if (!currentSlotGroup.asInterval().contains(slotRange)) {
                throw new IllegalStateException("(2) Not a valid slot slotRange.\n" + slotAssignment.toString() + " ==>> " + slotRange);
            }

            return slotAssignment.get(slotRange).getRight();
        }
    }

    public void releaseSlots(final SlotGroupAllocation sa) throws Exception {
        synchronized (this) {
            if (!slotAssignment.exist(sa.slotIDRange))
                return;
        }
        while (!slotAssignment.isDeepestInterval(sa.slotIDRange)) {
            synchronized (this) {
                wait();
            }
        }
        synchronized (this) {
            slotAssignment.remove(Preconditions.checkNotNull(sa.slotIDRange));
            notifyAll();
        }
    }

    // ---------------------------------------------------
    // ITERATION CONTROL FLOW.
    // ---------------------------------------------------

    public void localSync(final SlotContext slotContext) {
        try {
            final SlotGroup sg = getActiveSlotGroup();
            sg.sync(slotContext);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    public void globalSync(final SlotContext slotContext) {
        final NetEvents.NetEvent globalSyncEvent = new NetEvents.NetEvent(BSP_SYNC_BARRIER_EVENT, true);
        globalSyncEvent.setPayload(slotContext.programContext.programID);
        netManager.broadcastEvent(globalSyncEvent);
        slotContext.programContext.awaitGlobalSyncBarrier();
    }

    // ---------------------------------------------------
    // SLOT PARALLEL PRIMITIVES.
    // ---------------------------------------------------

    public Matrix.RowIterator parallelMatrixRowIterator(final Matrix matrix) {
        Preconditions.checkNotNull(matrix);
        final SlotContext slotContext = getSlotContext();
        final int scopeDOP = getScopeDOP();
        Matrix.PartitionShape shape = new MatrixByRowPartitioner(matrix.rows(), matrix.cols(), slotContext).getPartitionShape();
        return matrix.rowIterator((int) shape.rowOffset, (int) shape.rows);
    }

    // ---------------------------------------------------
    // UNUSED STUFF
    // ---------------------------------------------------

    /*public synchronized void releaseSlots(final SlotAllocation sa) throws Exception {
        //synchronized (monitor) {
            if (slotAssignment.exist(sa.slotIDRange)) {
                //while (!slotAssignment.isDeepestInterval(sa.slotIDRange)) {
                //    Thread.sleep(10);
                //}
                slotAssignment.remove(Preconditions.checkNotNull(sa.slotIDRange));
            }
        //}
    }*/

    /*public ExecutionFrame currentFrame() {

        final SlotContext sc = getSlotContext();

        return activeFrame[sc.slotID];
    }*/
}
