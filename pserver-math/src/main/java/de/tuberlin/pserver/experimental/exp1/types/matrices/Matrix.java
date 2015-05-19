package de.tuberlin.pserver.experimental.exp1.types.matrices;

import de.tuberlin.pserver.experimental.exp1.memory.TypedBuffer;
import de.tuberlin.pserver.experimental.exp1.memory.Types;

import java.io.Serializable;

public interface Matrix extends Serializable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    enum BlockLayout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract long numRows();

    public abstract long numCols();

    public abstract Types.TypeInformation getType();

    public abstract Types.TypeInformation getElementType();

    public abstract TypedBuffer getBuffer();

    public abstract byte[] getRow(final long row);

    public abstract byte[] getColumn(final long col);

    public abstract byte[] getElement(final long row, final long col);

    public abstract void setElement(final long row, final long col, final byte[] value);
}
