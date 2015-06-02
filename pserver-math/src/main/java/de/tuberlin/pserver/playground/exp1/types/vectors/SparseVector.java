package de.tuberlin.pserver.playground.exp1.types.vectors;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.playground.exp1.memory.TypedBuffer;
import de.tuberlin.pserver.playground.exp1.memory.Types;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SparseVector implements Vector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int length;

    private Types.TypeInformation elementTypeInformation;

    private final ConcurrentMap<Integer, byte[]> data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SparseVector(final int length, final Types.TypeInformation elementTypeInformation) {
        this.length = length;
        this.elementTypeInformation = Preconditions.checkNotNull(elementTypeInformation);
        this.data = new ConcurrentHashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int length() { return length; }

    @Override
    public Types.TypeInformation getType() { return null; }

    @Override
    public Types.TypeInformation getElementType() { return null; }

    @Override
    public TypedBuffer getBuffer() { return null; }

    @Override
    public byte[] getElement(final int pos) {
        final byte[] element = data.get(pos);
        if (element == null)
            return new byte[0];
        else
            return element;
    }

    @Override
    public void setElement(final int pos, final byte[] element) {
        data.put(pos, element);
    }
}
