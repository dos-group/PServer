package de.tuberlin.pserver.experimental.types.tests;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.experimental.memory.TypedBuffer;
import de.tuberlin.pserver.experimental.memory.Types;
import de.tuberlin.pserver.experimental.types.matrices.Matrix;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class DenseMatrixOld /*extends BufferValue*/ implements Matrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TypedBuffer buffer;

    protected final int rows;

    protected final int cols;

    protected final BlockLayout layout;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseMatrixOld(final int rows, final int cols,
                          final Types.TypeInformation elementTypeInfo,
                          final BlockLayout layout) {
        this.rows       = rows;
        this.cols       = cols;
        this.buffer     = new TypedBuffer(new Types.TypeInformation(
                Preconditions.checkNotNull(elementTypeInfo), rows * cols));
        this.layout     = Preconditions.checkNotNull(layout);
    }

    public DenseMatrixOld(final int rows, final int cols,
                          final TypedBuffer buffer,
                          final BlockLayout layout) {
        this.rows   = rows;
        this.cols   = cols;
        this.buffer = Preconditions.checkNotNull(buffer);
        this.layout = Preconditions.checkNotNull(layout);
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
        switch (layout) {
            case ROW_LAYOUT:
                return buffer.extractElementSequenceAsByteArray(cols * (int)row, cols);
            case COLUMN_LAYOUT:
                throw new NotImplementedException();
        }
        throw new IllegalStateException();
    }

    @Override
    public byte[] getColumn(final long col) {
        switch (layout) {
            case ROW_LAYOUT:
                throw new NotImplementedException();
            case COLUMN_LAYOUT: {
                final byte[] data = new byte[getElementType().size() * rows];
                for (int i = 0; i < rows; ++i)
                    System.arraycopy(buffer.getRawData(), (int)col * getElementType().size() + (i * cols * getElementType().size()),
                            data, i * getElementType().size(), getElementType().size());
                return data;
            }
        }
        throw new IllegalStateException();
    }

    public DenseVectorOld getRowAsDenseVector(final int row) {
        return new DenseVectorOld(getRow(row), getElementType());
    }

    public DenseVectorOld getColumnAsDenseVector(final int col) {
        return new DenseVectorOld(getColumn(col), getElementType());
    }

    @Override
    public byte[] getElement(final long row, final long col) {
        return buffer.extractElementAsByteArray(getPos((int)row, (int)col));
    }

    @Override
    public void setElement(final long row, final long col, final byte[] value) {
        buffer.put(getPos((int)row, (int)col), value);
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected int getPos(final int row, final int col) {
        switch (layout) {
            case ROW_LAYOUT:
                return (row * cols + col);
            case COLUMN_LAYOUT:
                return (col * rows + row);
        }
        throw new IllegalStateException();
    }

    protected int getOffset(final int row, final int col) {
        return getPos(row, col) * getBuffer().getType().getElementTypeInfo().size() + UnsafeOp.BYTE_ARRAY_OFFSET;
    }
}
