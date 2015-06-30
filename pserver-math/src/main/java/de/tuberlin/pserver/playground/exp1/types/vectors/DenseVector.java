package de.tuberlin.pserver.playground.exp1.types.vectors;

public class DenseVector /*extends ByteBufferValue implements Vector*/ {
/*
    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public TypedBuffer typedBuffer;

    public final Types.TypeInformation typeInfo;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseVector(final int length,
                       final Types.TypeInformation elementTypeInfo,
                       final boolean isAllocated) {
        super(length * elementTypeInfo.length(), isAllocated);
        this.typeInfo = new Types.TypeInformation(Preconditions.checkNotNull(elementTypeInfo), length);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void allocateMemory(final int instanceID) {
        Preconditions.checkState(!allocateMemory && buffer == null);
        buffer = new TypedBuffer(typeInfo);
        typedBuffer = (TypedBuffer) buffer;
    }

    // ---------------------------------------------------

    @Override
    public int length() { return typeInfo.getNumberOfElements(); }

    @Override
    public Types.TypeInformation getType() { return typeInfo; }

    @Override
    public Types.TypeInformation getElementType() { return typeInfo.getElementTypeInfo(); }

    @Override
    public TypedBuffer getBuffer() { return typedBuffer; }

    @Override
    public byte[] getElement(final int pos) {
        final byte[] value = new byte[typeInfo.getElementTypeInfo().length()];
        System.arraycopy(buffer.getRawData(), pos * typeInfo.getElementTypeInfo().length(), value, 0, value.length);
        return value;
    }

    @Override
    public void setElement(final int pos, final byte[] value) {
        Preconditions.checkArgument(value.length == typeInfo.getElementTypeInfo().length());
        System.arraycopy(value, 0, buffer.getRawData(), pos * typeInfo.getElementTypeInfo().length(), value.length);
    }

    // ---------------------------------------------------

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && obj instanceof DenseVectorOld
                && length() == ((DenseVectorOld) obj).length()
                && Arrays.equals(((DenseVectorOld) obj).buffer.getRawData(), buffer.getRawData());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(buffer.getRawData()); }
    */
}
