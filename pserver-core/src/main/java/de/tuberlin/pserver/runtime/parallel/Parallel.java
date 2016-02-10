package de.tuberlin.pserver.runtime.parallel;



import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.matrix.f32.Matrix32F;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class Parallel {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static ParallelRuntime parallelRuntime;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Parallel() { Preconditions.checkState(parallelRuntime != null); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static void setMCRuntime(final ParallelRuntime parallelRuntime) { Parallel.parallelRuntime = parallelRuntime; }

    // ---------------------------------------------------
    // MCRuntime Primitives.
    // ---------------------------------------------------

    public static int id() { return parallelRuntime.currentSlot().slotID; }

    public static void Serial(final ParallelBody body)
            throws Exception {

        final WorkerSlot ws = parallelRuntime.currentSlot();
        if (ws.slotID == 0 && !ws.empty()) {
            ws.run(1, body);
        } else
            throw new IllegalStateException();
    }

    public static void Do(final ParallelBody parallelBody)
            throws Exception {

        Do(parallelRuntime.getNumOfWorkerSlots(), parallelBody);
    }

    public static void Do(final int dop, final ParallelBody parallelBody)
            throws Exception {

        final WorkerSlot ws = parallelRuntime.currentSlot();
        if (ws == null || (ws.slotID == 0 && ws.empty() && dop > 1)) {
            executeByWorkerSlots(dop, parallelBody);
        } else {
            ws.run(dop, parallelBody);
        }
    }

    public static void For(final long numIterations, final ParallelForBody<Long> body)
            throws Exception {

        For(parallelRuntime.getNumOfWorkerSlots(), 0, numIterations, body);
    }

    public static void For(final long start, final long end, final ParallelForBody<Long> body)
            throws Exception {

        For(parallelRuntime.getNumOfWorkerSlots(), start, end, body);
    }

    public static void For(final int dop, final long start, final long end, final ParallelForBody<Long> body)
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

        For(parallelRuntime.getNumOfWorkerSlots(), elements, body);
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

    public static void For(final Matrix32F m, final ParallelForMatrixBody body) throws Exception {
        Matrix32F.RowIterator iter = m.rowIterator();

        /*if (m.rows() / parallelRuntime.getNumOfWorkerSlots() < 1) {
            while (iter.hasNext()) {
                final Matrix<V> row = iter.get();
                iter.next();
                for (int j = 0; j < m.cols(); ++j)
                    body.perform(i.intValue(), j, row.get(0, j));
            }
        }*/

        For(0, iter.size(), (i) -> {
            Preconditions.checkState(iter.hasNext(), "iter.size = " + iter.size() + ", fetched = " + i);
            final Matrix32F row = iter.get();
            iter.next();
            for (int j = 0; j < m.cols(); ++j)
                body.perform(i.intValue(), j, row.get(0, j));
        });
    }

    public static void For(final Matrix32F m, final ParallelForRowMatrixBody body) throws Exception {
        For(0, m.rowIterator().size(), body::perform);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private static void executeByWorkerSlots(final int dop, final ParallelBody parallelBody) throws Exception {
        for (int i = dop - 1; i >= 0; --i) {
            parallelRuntime.getWorkerSlots()[i].run(dop, parallelBody);
        }
    }
}