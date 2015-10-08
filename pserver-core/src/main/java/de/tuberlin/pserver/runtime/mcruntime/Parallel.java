package de.tuberlin.pserver.runtime.mcruntime;



import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class Parallel {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static MCRuntime mcRuntime;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Parallel() { Preconditions.checkState(mcRuntime != null); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static void setMCRuntime(final MCRuntime mcRuntime) { Parallel.mcRuntime = mcRuntime; }

    // ---------------------------------------------------
    // MCRuntime Primitives.
    // ---------------------------------------------------

    public static int id() { return mcRuntime.currentSlot().slotID; }

    public static void Serial(final ParallelBody body)
            throws Exception {

        final WorkerSlot ws = mcRuntime.currentSlot();
        if (ws.slotID == 0 && !ws.empty()) {
            ws.run(1, body);
        } else
            throw new IllegalStateException();
    }

    public static void Do(final ParallelBody parallelBody)
            throws Exception {

        Do(mcRuntime.getNumOfWorkerSlots(), parallelBody);
    }

    public static void Do(final int dop, final ParallelBody parallelBody)
            throws Exception {

        final WorkerSlot ws = mcRuntime.currentSlot();
        if (ws.slotID == 0 && ws.empty() && dop > 1) {
            executeByWorkerSlots(dop, parallelBody);
        } else {
            ws.run(dop, parallelBody);
        }
    }

    public static void For(final int start, final int end, final ParallelForBody<Long> body)
            throws Exception {

        For(mcRuntime.getNumOfWorkerSlots(), start, end, body);
    }

    public static void For(final int dop, final int start, final int end, final ParallelForBody<Long> body)
            throws Exception {

        Do(dop, () -> {
            final double elementsPerCore = (double) (end - start) / dop;
            final long elementOffset = (int) Math.ceil(elementsPerCore * id());
            final long numElements = (int) (Math.ceil(elementsPerCore * (id() + 1)) - elementOffset);
            for (long i = elementOffset; i < elementOffset + numElements; ++i)
                body.perform(i);
        });
    }

    public static <T> void For(final Iterable<T> elements, final ParallelForBody<T> body)
            throws Exception {

        For(mcRuntime.getNumOfWorkerSlots(), elements, body);
    }

    public static <T> void For(final int dop, final Iterable<T> elements, final ParallelForBody<T> body)
            throws Exception {

        final List<Callable<Void>> invokes = new ArrayList<>();
        for (final T e : elements) {
            invokes.add(() -> {
                body.perform(e);
                return null;
            });
        }

        For(dop, 0, invokes.size() - 1, (i) -> {
            invokes.get(i.intValue()).call();
        });
    }

    public static void For(final Matrix m, final ParallelForMatrixBody body) throws Exception {
        For(0, (int)m.rows(), (i) -> {
            for (int j = 0; j < m.cols(); ++j)
                body.perform(i.intValue(), j, m.get(i, j));
        });
    }

    public static void For(final Matrix m, final ParallelForRowMatrixBody body) throws Exception {
        For(0, (int)m.rows(), body::perform);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private static void executeByWorkerSlots(final int dop, final ParallelBody parallelBody) throws Exception {
        for (int i = dop - 1; i >= 0; --i) {
            mcRuntime.getWorkerSlots()[i].run(dop, parallelBody);
        }
    }
}