package de.tuberlin.pserver.types;


import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class RemoteMatrixStub extends AbstractMatrix {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public RemoteMatrixStub(final SlotContext slotContext,
                            final String name,
                            final Matrix matrix) {

        super(matrix.rows(), matrix.cols(), matrix.layout());

        final NetManager netManager = slotContext.runtimeContext.netManager;

        netManager.addEventListener("get_request_" + name, event -> {
            final NetEvents.NetEvent getRequestEvent = (NetEvents.NetEvent) event;
            @SuppressWarnings("unchecked")
            final Pair<Long, Long> pos = (Pair<Long, Long>) getRequestEvent.getPayload();
            final NetEvents.NetEvent getResponseEvent = new NetEvents.NetEvent("get_response_" + name);
            getResponseEvent.setPayload(matrix.get(pos.getLeft(), pos.getRight()));
            netManager.sendEvent(((NetEvents.NetEvent) event).srcMachineID, getResponseEvent);
        });

        netManager.addEventListener("put_request_" + name, event -> {
            final NetEvents.NetEvent putRequestEvent = (NetEvents.NetEvent) event;
            @SuppressWarnings("unchecked")
            final Triple<Long, Long, Double> pos = (Triple<Long, Long, Double>) putRequestEvent.getPayload();
            matrix.set(pos.getLeft(), pos.getMiddle(), pos.getRight());
        });

    }

    // ---------------------------------------------------
    // Public Methods..
    // ---------------------------------------------------

    @Override public double get(long row, long col) { throw new UnsupportedOperationException(); }

    @Override public void set(long row, long col, double value) { throw new UnsupportedOperationException(); }

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

    @Override public Matrix assign(long row, long col, Matrix m) { throw new UnsupportedOperationException(); }

    @Override protected Matrix newInstance(long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override public double[] toArray() { throw new UnsupportedOperationException(); }

    @Override public void setArray(double[] data) { throw new UnsupportedOperationException(); }

    @Override public RowIterator rowIterator() { throw new UnsupportedOperationException(); }

    @Override public RowIterator rowIterator(int startRow, int endRow) { throw new UnsupportedOperationException(); }
}
