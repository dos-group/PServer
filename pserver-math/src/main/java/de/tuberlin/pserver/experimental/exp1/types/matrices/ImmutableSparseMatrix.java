package de.tuberlin.pserver.experimental.exp1.types.matrices;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.experimental.exp1.memory.TypedBuffer;
import de.tuberlin.pserver.experimental.exp1.memory.Types;
import de.tuberlin.pserver.experimental.exp1.tuples.Tuple3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ImmutableSparseMatrix implements Matrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TypedBuffer buffer;

    protected final int rows;

    protected final int cols;

    protected final int[] rowIndex;

    protected final int[] colIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ImmutableSparseMatrix(final int rows, final int cols, final Types.TypeInformation typeInfo, final List<Tuple3> data) {
        this.rows = rows;
        this.cols = cols;
        final Types.TypeInformation arrayType = new Types.TypeInformation(Preconditions.checkNotNull(typeInfo), data.size());
        this.buffer = new TypedBuffer(arrayType);
        Collections.sort(data);
        this.rowIndex = new int[data.size()];
        this.colIndex = new int[data.size()];
        for (int i = 0; i < data.size(); ++i) {
            @SuppressWarnings("unchecked")
            final Tuple3<Integer, Integer, Object> element = (Tuple3<Integer, Integer, Object>) data.get(i);
            if (element._3 instanceof Types.ByteArraySerializable) {
                buffer.put(i, ((Types.ByteArraySerializable) element._3).toByteArray());
            } else {
                // TODO:
            }
            rowIndex[i] = element._1;
            colIndex[i] = element._2;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long numRows() { return rows; }

    @Override
    public long numCols() { return cols; }

    @Override
    public Types.TypeInformation getType() { return buffer.getType(); }

    @Override
    public Types.TypeInformation getElementType() { return buffer.getType().getElementTypeInfo(); }

    @Override
    public TypedBuffer getBuffer() { return buffer; }

    @Override
    public byte[] getRow(final long row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getColumn(final long col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getElement(final long row, final long col) {
        final int y = Arrays.binarySearch(rowIndex, (int)row);
        final int x = Arrays.binarySearch(colIndex, (int)col);
        if (y >= 0 && x >= 0)
            return buffer.extractElementAsByteArray(colIndex.length * y + x);
        else
            return new byte[buffer.getType().getElementTypeInfo().size()];
    }

    @Override
    public void setElement(final long row, final long col, final byte[] value) {
        throw new UnsupportedOperationException();
    }

    // ---------------------------------------------------

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof ImmutableSparseMatrix)) return false;
        if (numRows() != ((ImmutableSparseMatrix) obj).numRows() ||
                numCols() != ((ImmutableSparseMatrix) obj).numCols()) return false;
        return Arrays.equals(((ImmutableSparseMatrix) obj).buffer.getRawData(), buffer.getRawData());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(buffer.getRawData()); }
}
