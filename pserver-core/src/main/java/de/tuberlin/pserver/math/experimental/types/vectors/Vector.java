package de.tuberlin.pserver.math.experimental.types.vectors;

import de.tuberlin.pserver.math.experimental.memory.TypedBuffer;
import de.tuberlin.pserver.math.experimental.memory.Types;

import java.io.Serializable;

public interface Vector extends Serializable {

    public abstract int length();

    public abstract Types.TypeInformation getType();

    public abstract Types.TypeInformation getElementType();

    public abstract TypedBuffer getBuffer();

    public abstract byte[] getElement(final int pos);

    public abstract void setElement(final int pos, final byte[] value);
}
