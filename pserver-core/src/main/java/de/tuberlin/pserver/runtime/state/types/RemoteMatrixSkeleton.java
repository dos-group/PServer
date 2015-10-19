package de.tuberlin.pserver.runtime.state.types;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.net.NetManager;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Layout;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class RemoteMatrixSkeleton<V extends Number> implements Matrix<V> {

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

        //super(rows, cols, layout);

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

    /*@Override
    public void set(long row, long col, V value) {
        final NetEvents.NetEvent putRequestEvent = new NetEvents.NetEvent("put_request_" + name);
        putRequestEvent.setPayload(Triple.of(row, col, value));
        netManager.sendEvent(atNodeID, putRequestEvent);
    }

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

    }*/


    @Override
    public Matrix<V> copy() {
        return null;
    }

    @Override
    public Matrix<V> copy(long rows, long cols) {
        return null;
    }

    @Override
    public void set(long row, long col, V value) {

    }

    @Override
    public Matrix<V> setDiagonalsToZero() {
        return null;
    }

    @Override
    public Matrix<V> setDiagonalsToZero(Matrix<V> B) {
        return null;
    }

    @Override
    public void setArray(Object data) {

    }

    @Override
    public V get(long index) {
        return null;
    }

    @Override
    public V get(long row, long col) {
        return null;
    }


    @Override
    public Matrix<V> getRow(long row) {
        return null;
    }

    @Override
    public Matrix<V> getRow(long row, long from, long to) {
        return null;
    }

    @Override
    public Matrix<V> getCol(long col) {
        return null;
    }

    @Override
    public Matrix<V> getCol(long col, long from, long to) {
        return null;
    }

    @Override
    public Object toArray() {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(UnaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(UnaryOperator<V> f, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(Matrix<V> B, BinaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(Matrix<V> B, BinaryOperator<V> f, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(MatrixElementUnaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(MatrixElementUnaryOperator<V> f, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> applyOnNonZeroElements(MatrixElementUnaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnNonZeroElements(MatrixElementUnaryOperator<V> f, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> assign(Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> assign(V v) {
        return null;
    }

    @Override
    public Matrix<V> assignRow(long row, Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> assignColumn(long col, Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> assign(long rowOffset, long colOffset, Matrix<V> m) {
        return null;
    }

    @Override
    public V aggregate(BinaryOperator<V> combiner, UnaryOperator<V> mapper, Matrix<V> result) {
        return null;
    }

    @Override
    public Matrix<V> aggregateRows(MatrixAggregation<V> f) {
        return null;
    }

    @Override
    public Matrix<V> aggregateRows(MatrixAggregation<V> f, Matrix<V> result) {
        return null;
    }

    @Override
    public V sum() {
        return null;
    }

    @Override
    public Matrix<V> add(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> add(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToRows(Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToRows(Matrix<V> v, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToCols(Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToCols(Matrix<V> v, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> sub(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> sub(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> mul(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> mul(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> scale(V a) {
        return null;
    }

    @Override
    public Matrix<V> scale(V a, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> transpose() {
        return null;
    }

    @Override
    public Matrix<V> transpose(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> invert() {
        return null;
    }

    @Override
    public Matrix<V> invert(Matrix<V> B) {
        return null;
    }

    @Override
    public V norm(int p) {
        return null;
    }

    @Override
    public V dot(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        return null;
    }

    @Override
    public Matrix<V> concat(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> concat(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public RowIterator<V, Matrix<V>> rowIterator() {
        return null;
    }

    @Override
    public RowIterator<V, Matrix<V>> rowIterator(long startRow, long endRow) {
        return null;
    }

    @Override
    public long rows() {
        return 0;
    }

    @Override
    public long cols() {
        return 0;
    }

    @Override
    public long sizeOf() {
        return 0;
    }

    @Override
    public Layout layout() {
        return null;
    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public void setOwner(Object owner) {

    }

    @Override
    public Object getOwner() {
        return null;
    }
}
