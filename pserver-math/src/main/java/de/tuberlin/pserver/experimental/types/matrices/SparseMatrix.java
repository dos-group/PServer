package de.tuberlin.pserver.experimental.types.matrices;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.experimental.memory.TypedBuffer;
import de.tuberlin.pserver.experimental.memory.Types;
import de.tuberlin.pserver.experimental.tuples.Tuple2;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SparseMatrix implements Matrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int rows;

    private int cols;

    private Types.TypeInformation elementTypeInformation;

    private final ConcurrentMap<Tuple2<Integer, Integer>, byte[]> data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseMatrix(final int rows, final int cols, final Types.TypeInformation elementTypeInformation) {
        this.rows = rows;
        this.cols = cols;
        this.elementTypeInformation = Preconditions.checkNotNull(elementTypeInformation);
        this.data = new ConcurrentHashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long numRows() { return rows; }

    @Override
    public long numCols() { return cols; }

    @Override
    public Types.TypeInformation getType() { return null; }

    @Override
    public Types.TypeInformation getElementType() { return elementTypeInformation; }

    @Override
    public TypedBuffer getBuffer() { return null; }

    @Override
    public byte[] getRow(final long row) { throw new NotImplementedException(); }

    @Override
    public byte[] getColumn(final long col) {
        throw new NotImplementedException();
    }

    @Override
    public byte[] getElement(final long row, final long col) {
        final byte[] element = data.get(new Tuple2<>((int)row, (int)col));
        if (element == null)
            return new byte[0];
        else
            return element;
    }

    @Override
    public void setElement(final long row, final long col, final byte[] value) {
        data.put(new Tuple2<>((int) row, (int) col), Preconditions.checkNotNull(value));
    }
}
