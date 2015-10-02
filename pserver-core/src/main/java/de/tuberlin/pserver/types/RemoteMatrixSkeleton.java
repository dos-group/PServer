package de.tuberlin.pserver.types;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class RemoteMatrixSkeleton extends AbstractMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final String name;

    private final int atNodeID;

    private final long rows;

    private final long cols;

    private final Layout layout;

    private final Format format;

    // ---------------------------------------------------

    private final NetManager netManager;

    private final CyclicBarrier barrier = new CyclicBarrier(2);

    private final MutableDouble returnedValue = new MutableDouble(Double.NaN);

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public RemoteMatrixSkeleton(final ProgramContext programContext,
                                final String name,
                                final int atNodeID,
                                final long rows,
                                final long cols,
                                final Format format,
                                final Layout layout) {

        super(rows, cols, layout);

        this.programContext = Preconditions.checkNotNull(programContext);
        this.name           = Preconditions.checkNotNull(name);
        this.atNodeID       = atNodeID;
        this.rows           = rows;
        this.cols           = cols;
        this.format         = format;
        this.layout         = layout;

        this.netManager    = programContext.runtimeContext.netManager;

        netManager.addEventListener("get_response_" + name, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                @SuppressWarnings("unchecked")
                final Double result = (Double) event.getPayload();
                returnedValue.setValue(result);
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    // ---------------------------------------------------
    // Public Methods..
    // ---------------------------------------------------

    @Override
    public double get(long row, long col) {
        final NetEvents.NetEvent getRequestEvent = new NetEvents.NetEvent("get_request_" + name);
        getRequestEvent.setPayload(Pair.of(row, col));
        netManager.sendEvent(atNodeID, getRequestEvent);
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        return returnedValue.doubleValue();

    }

    @Override
    public void set(long row, long col, double value) {
        final NetEvents.NetEvent putRequestEvent = new NetEvents.NetEvent("put_request_" + name);
        putRequestEvent.setPayload(Triple.of(row, col, value));
        netManager.sendEvent(atNodeID, putRequestEvent);
    }

    // ---------------------------------------------------

    @Override public double get(long index) { throw new UnsupportedOperationException(); }

    @Override public Matrix getRow(long row) { throw new UnsupportedOperationException(); }

    @Override public Matrix getRow(long row, long from, long to) { throw new UnsupportedOperationException(); }

    @Override public Matrix getCol(long col) { throw new UnsupportedOperationException(); }

    @Override public Matrix getCol(long col, long from, long to) { throw new UnsupportedOperationException(); }

    @Override public Matrix assign(Matrix m) { throw new UnsupportedOperationException(); }

    @Override public Matrix assign(double v) { throw new UnsupportedOperationException(); }

    @Override public Matrix assignRow(long row, Matrix v) { throw new UnsupportedOperationException(); }

    @Override public Matrix assignColumn(long col, Matrix v) { throw new UnsupportedOperationException(); }

    @Override public Matrix copy() { throw new UnsupportedOperationException(); }

    @Override public Matrix subMatrix(long row, long col, long rowSize, long colSize) { throw new UnsupportedOperationException(); }

    @Override public Matrix assign(long rowOffset, long colOffset, Matrix m) { throw new UnsupportedOperationException(); }

    @Override protected Matrix newInstance(long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override public double[] toArray() { throw new UnsupportedOperationException(); }

    @Override public void setArray(double[] data) { throw new UnsupportedOperationException(); }

    @Override public RowIterator rowIterator() { throw new UnsupportedOperationException(); }

    @Override public RowIterator rowIterator(int startRow, int endRow) { throw new UnsupportedOperationException(); }
}
