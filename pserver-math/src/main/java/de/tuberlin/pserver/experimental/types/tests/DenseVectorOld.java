package de.tuberlin.pserver.experimental.types.tests;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.experimental.memory.TypedBuffer;
import de.tuberlin.pserver.experimental.memory.Types;
import de.tuberlin.pserver.experimental.types.vectors.Vector;

import java.util.Arrays;

public class DenseVectorOld implements Vector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final TypedBuffer buffer;

    public final Types.TypeInformation typeInfo;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseVectorOld(final byte[] data, final Types.TypeInformation elementTypeInfo) {
        this(new TypedBuffer(new Types.TypeInformation(elementTypeInfo, data.length / elementTypeInfo.size())));
        this.buffer.setRawData(data);
    }

    public DenseVectorOld(final TypedBuffer buffer) {
        this.buffer = Preconditions.checkNotNull(buffer);
        this.typeInfo = buffer.getType();
    }

    public DenseVectorOld(final Types.TypeInformation typeInfo) {
        this.typeInfo = Preconditions.checkNotNull(typeInfo);
        this.buffer = new TypedBuffer(typeInfo);
    }

    public DenseVectorOld(final int size, Types.TypeInformation elementTypeInfo) {
        this.typeInfo = new Types.TypeInformation(Preconditions.checkNotNull(elementTypeInfo), size);
        this.buffer = new TypedBuffer(typeInfo);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int length() { return typeInfo.getNumberOfElements(); }

    @Override
    public Types.TypeInformation getType() { return typeInfo; }

    @Override
    public Types.TypeInformation getElementType() { return typeInfo.getElementTypeInfo(); }

    @Override
    public TypedBuffer getBuffer() { return buffer; }

    @Override
    public byte[] getElement(final int pos) {
        final byte[] value = new byte[typeInfo.getElementTypeInfo().size()];
        System.arraycopy(buffer.getRawData(), pos * typeInfo.getElementTypeInfo().size(), value, 0, value.length);
        return value;
    }

    @Override
    public void setElement(final int pos, final byte[] value) {
        Preconditions.checkArgument(value.length == typeInfo.getElementTypeInfo().size());
        System.arraycopy(value, 0, buffer.getRawData(), pos * typeInfo.getElementTypeInfo().size(), value.length);
    }

    // ---------------------------------------------------

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof DenseVectorOld)) return false;
        if (length() != ((DenseVectorOld) obj).length()) return false;
        return Arrays.equals(((DenseVectorOld) obj).buffer.getRawData(), buffer.getRawData());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(buffer.getRawData()); }
}
