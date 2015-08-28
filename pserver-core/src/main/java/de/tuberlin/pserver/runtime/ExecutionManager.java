package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.NestedIntervalTree;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.dsl.controlflow.CFStatement;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.types.DistributedMatrix;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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

    public static final class SlotAllocation {

        public NestedIntervalTree.Interval range;

        public ExecutionFrame[] frames;

        public SlotAllocation(final NestedIntervalTree.Interval range) {

            this.range = Preconditions.checkNotNull(range);

            this.frames = new ExecutionFrame[(range.high - range.low) + 1];
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

    public int numOfSlots;

    private final NetManager netManager;

    private final Map<UUID, MLProgramContext> programContextMap;

    private final Map<Long, SlotContext> slotContextMap;

    // DSL Runtime.

    private final Object monitor = new Object();

    private final ExecutionFrame[] activeFrame;

    private final ThreadLocal<MutableInt> currentFrameLevel;

    private NestedIntervalTree<SlotAllocation> slotAssignment;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ExecutionManager(final int numOfSlots, final NetManager netManager) {

        this.numOfSlots = numOfSlots;

        this.netManager = Preconditions.checkNotNull(netManager);

        this.programContextMap = new ConcurrentHashMap<>();

        this.slotContextMap = new ConcurrentHashMap<>();

        this.netManager.addEventListener(BSP_SYNC_BARRIER_EVENT, event ->
                programContextMap.get(event.getPayload()).globalSyncBarrier.countDown());

        // DSL Runtime.

        this.activeFrame = new ExecutionFrame[numOfSlots];

        this.currentFrameLevel = new ThreadLocal<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void registerJob(final UUID jobID, final MLProgramContext programContext) {
        programContextMap.put(Preconditions.checkNotNull(jobID), Preconditions.checkNotNull(programContext));
    }

    public MLProgramContext getJob(final UUID jobID) {
        return programContextMap.get(Preconditions.checkNotNull(jobID));
    }

    public void unregisterJob(final UUID jobID) {
        programContextMap.remove(Preconditions.checkNotNull(jobID));
    }

    public synchronized void registerSlotContext(final SlotContext ic) {
        slotContextMap.put(Thread.currentThread().getId(), Preconditions.checkNotNull(ic));
    }

    public synchronized SlotContext getSlotContext() {
        return slotContextMap.get(Thread.currentThread().getId());
    }

    public synchronized void unregisterSlotContext() {
        slotContextMap.remove(Thread.currentThread().getId());
    }

    public void setNumOfSlots(final int numOfSlots) {

        this.numOfSlots = numOfSlots;

        final NestedIntervalTree.Interval in = new NestedIntervalTree.Interval(0, numOfSlots - 1);

        final SlotAllocation sa = new SlotAllocation(in);

        this.slotAssignment = new NestedIntervalTree<>(in, sa);
    }

    public int getNumOfSlots() { return numOfSlots; }

    // ---------------------------------------------------
    // DSL RUNTIME MANAGEMENT.
    // ---------------------------------------------------

    public void pushFrame(final CFStatement statement) {

        final SlotContext ic = getSlotContext();

        if (currentFrameLevel.get() == null) {

            currentFrameLevel.set(new MutableInt(0));
        }

        currentFrameLevel.get().increment();

        final ExecutionFrame newFrame = new ExecutionFrame(currentFrameLevel.get().getValue(), activeFrame[ic.slotID], statement, ic);

        activeFrame[ic.slotID] = newFrame;

        synchronized (monitor) {

            final Pair<NestedIntervalTree.Interval, SlotAllocation> slotGroup = slotAssignment.get(new NestedIntervalTree.Point(ic.slotID));

            int index = ic.slotID - slotGroup.getLeft().low;

            slotGroup.getRight().frames[index] = newFrame;
        }
    }

    public void popFrame() {

        final SlotContext ic = getSlotContext();

        activeFrame[ic.slotID] = activeFrame[ic.slotID].parent;

        currentFrameLevel.get().decrement();
    }

    public ExecutionFrame currentFrame() {

        final SlotContext ic = getSlotContext();

        return activeFrame[ic.slotID];
    }

    public int getScopeDOP() {

        final SlotContext ic = getSlotContext();

        synchronized (monitor) {

            return slotAssignment.get(new NestedIntervalTree.Point(ic.slotID)).getRight().size();
        }
    }

    public Pair<Integer, Integer> getAvailableSlotRangeForScope() {

        final SlotContext ic = getSlotContext();

        synchronized (monitor) {

            final NestedIntervalTree.Interval slots = slotAssignment.get(new NestedIntervalTree.Point(ic.slotID)).getLeft();

            return Pair.of(slots.low, slots.high);
        }
    }

    // ---------------------------------------------------
    // SLOT ASSIGNMENT.
    // ---------------------------------------------------

    public SlotAllocation allocSlots(final int low, final int high) {

        final NestedIntervalTree.Interval range = new NestedIntervalTree.Interval(low, high);

        synchronized (monitor) {

            if (!slotAssignment.exist(range)) {

                if (!slotAssignment.isValid(range))
                    throw new IllegalStateException();

                final SlotAllocation sa = new SlotAllocation(range);

                if (!slotAssignment.put(range, sa))
                    throw new IllegalStateException();

                return sa;

            } else {

                return slotAssignment.get(range).getRight();
            }
        }
    }

    public void releaseSlots(final SlotAllocation sa) {

        synchronized (monitor) {

            if (slotAssignment.exist(sa.range)) {

                slotAssignment.remove(Preconditions.checkNotNull(sa.range));
            }
        }
    }

    // ---------------------------------------------------
    // ITERATION CONTROL FLOW.
    // ---------------------------------------------------

    public void globalSync() {
        final SlotContext slotContext = getSlotContext();
        final NetEvents.NetEvent globalSyncEvent = new NetEvents.NetEvent(BSP_SYNC_BARRIER_EVENT);
        globalSyncEvent.setPayload(slotContext.programContext.programID);
        netManager.broadcastEvent(globalSyncEvent);
        try {
            slotContext.programContext.globalSyncBarrier.await();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
        if (slotContext.programContext.globalSyncBarrier.getCount() == 0) {
            slotContext.programContext.globalSyncBarrier.reset();
        } else {
            throw new IllegalStateException();
        }
    }

    public void localSync(final SlotContext slotContext) {
        //final SlotContext slotContext = getSlotContext();
        try {
            slotContext.programContext.localSyncBarrier.await();
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    // ---------------------------------------------------
    // SLOT PARALLEL PRIMITIVES.
    // ---------------------------------------------------

    public Matrix.RowIterator parallelMatrixRowIterator(final Matrix matrix) {
        Preconditions.checkNotNull(matrix);
        final SlotContext slotContext = getSlotContext();
        final int scopeDOP = getScopeDOP();
        final Pair<Integer, Integer> range = getAvailableSlotRangeForScope();
        int startOffset, endOffset, blockSize;
        if (matrix.getClass() == DistributedMatrix.class) {
            final DistributedMatrix dm = (DistributedMatrix)matrix;
            blockSize   = (int) dm.partitionNumRows() / scopeDOP;
            startOffset = (int)(dm.partitionBaseRowOffset() + (slotContext.slotID * blockSize));
            endOffset   = startOffset + blockSize - 1;
            endOffset   = (slotContext.slotID == range.getRight())
                    ? endOffset + (int) dm.partitionNumRows() % scopeDOP
                    : endOffset;
        } else {
            blockSize   = (int) matrix.rows() / scopeDOP;
            startOffset = slotContext.slotID * blockSize;
            endOffset   = (slotContext.slotID * blockSize + blockSize - 1);
            endOffset   = (slotContext.slotID == range.getRight())
                    ? endOffset + (int) matrix.rows() % scopeDOP
                    : endOffset;
        }
        return matrix.rowIterator(startOffset, endOffset);
    }

    public Vector.ElementIterator parallelVectorElementIterator(final Vector vector) {
        Preconditions.checkNotNull(vector);
        final SlotContext slotContext = getSlotContext();
        final int scopeDOP = getScopeDOP();
        final Pair<Integer, Integer> range = getAvailableSlotRangeForScope();
        int startOffset, endOffset, blockSize;
            blockSize   = (int) vector.length() / scopeDOP;
            startOffset = slotContext.slotID * blockSize;
            endOffset   = (slotContext.slotID * blockSize + blockSize - 1);
            endOffset   = (slotContext.slotID == range.getRight())
                    ? endOffset + (int) vector.length() % scopeDOP
                    : endOffset;
        return vector.elementIterator(startOffset, endOffset);
    }
}
