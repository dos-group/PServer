package de.tuberlin.pserver.runtime.parallel;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.ds.Stack;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerSlot {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class StackFrame {

        public final SlotGroup currentSlotGroup;

        public final StackFrame parentFrame;

        public final ParallelBody body;

        public StackFrame(final StackFrame parentFrame,
                          final SlotGroup currentSlotGroup,
                          final ParallelBody body) {

            this.parentFrame = parentFrame;

            this.currentSlotGroup = currentSlotGroup;

            this.body = Preconditions.checkNotNull(body);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static MCRuntime mcRuntime;

    // ---------------------------------------------------

    public final int slotID;

    private final BlockingQueue<Pair<Integer, ParallelBody>> parallelTasks;

    private final Stack<StackFrame> callStack;

    private final Thread workerThread;

    private final AtomicBoolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public WorkerSlot(final int slotID) {

        this.slotID = slotID;

        this.parallelTasks = new LinkedBlockingQueue<>();

        this.callStack = new Stack<>();

        this.isRunning = new AtomicBoolean(true);

        if (slotID > 0) {

            this.workerThread = new Thread(slot());

            this.workerThread.start();

        } else {

            this.workerThread = Thread.currentThread();
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int size() { return callStack.size(); }

    public boolean empty() { return callStack.size() == 0; }

    // ---------------------------------------------------

    public void run(final int dop, final ParallelBody parallelBody) throws Exception {

        if (slotID == 0) {

            _call(dop, parallelBody);

            if (callStack.size() == 0) {

                boolean finished;
                do {
                    finished = true;
                    for (final WorkerSlot ws : mcRuntime.getWorkerSlots()) {
                        finished &= ws.empty();
                    }
                } while(!finished);

                mcRuntime.clearAllocations();
            }

        } else {

            if (callStack.size() == 0) {

                parallelTasks.add(Pair.of(dop, parallelBody));

            } else {

                _call(dop, parallelBody);
            }
        }
    }

    public void shutdown() {

        workerThread.interrupt();

        isRunning.set(false);
    }

    @Override public String toString() { return "" + slotID; }

    public static void setMCRuntime(final MCRuntime mcRuntime) { WorkerSlot.mcRuntime = mcRuntime; }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Runnable slot() {

        return () -> {

            mcRuntime.registerWorkerSlot(this);

            while (isRunning.get()) {

                try {

                    final Pair<Integer, ParallelBody> task = parallelTasks.take();

                    if (task != null) {

                        _call(task.getLeft(), task.getRight());
                    }

                } catch (Exception e) {

                    isRunning.set(false);

                    if (!(e instanceof InterruptedException))
                        throw new IllegalStateException(e);
                }
            }
        };
    }

    private StackFrame _push(final int dop, final ParallelBody body) throws Exception {

        final SlotGroup currentSlotGroup = mcRuntime.allocSlots(dop);

        final StackFrame frame = new StackFrame(callStack.peek(), currentSlotGroup, body);

        callStack.push(frame);

        return frame;
    }

    private void _pop() throws Exception {

        //System.out.println("slot = " + id + " - POP - callStack = " + (callStack.size() - 1));

        callStack.pop();
    }

    private void _call(final int dop, final ParallelBody body) throws Exception {

        final StackFrame frame = _push(dop, body);

        if (frame.currentSlotGroup.contains(slotID)) {

            frame.currentSlotGroup.barrier();

            frame.body.perform();

            frame.currentSlotGroup.barrier();
        }

        _pop();
    }

    // ---------------------------------------------------
    // Debugging.
    // ---------------------------------------------------

    public synchronized String getJavaStackTrace() {

        final  StringBuilder sb = new StringBuilder();

        for (final StackTraceElement ste : workerThread.getStackTrace())
            sb.append(ste.toString()).append('\n');

        return sb.toString();
    }

    public String getThreadName() { return workerThread.getName(); }
}
