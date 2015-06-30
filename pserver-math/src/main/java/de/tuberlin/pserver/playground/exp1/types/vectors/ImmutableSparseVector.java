package de.tuberlin.pserver.playground.exp1.types.vectors;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.playground.exp1.memory.TypedBuffer;
import de.tuberlin.pserver.playground.exp1.memory.Types;
import de.tuberlin.pserver.playground.exp1.tuples.Tuple2;

import java.util.*;

public class ImmutableSparseVector implements Vector {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final int sparseSize;

    protected final TypedBuffer buffer;

    protected final Types.TypeInformation typeInfo;

    protected final int[] arrayIndex;

    protected final Map<Integer,Integer> hashIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ImmutableSparseVector(final int size, final Types.TypeInformation typeInfo, final List<Tuple2> data, final boolean useHashIndex) {
        Preconditions.checkArgument(size > 0);
        this.sparseSize = size;
        this.typeInfo = Preconditions.checkNotNull(typeInfo);
        final Types.TypeInformation arrayType = new Types.TypeInformation(typeInfo, data.size());
        this.buffer = new TypedBuffer(arrayType);

        if (useHashIndex) {
            this.arrayIndex = null;
            this.hashIndex = new HashMap<>(data.size());
        } else {
            Collections.sort(data);
            this.arrayIndex = new int[data.size()];
            this.hashIndex = null;
        }

        for (int i = 0; i < data.size(); ++i) {
            @SuppressWarnings("unchecked")
            final Tuple2<Integer,Object> element = (Tuple2<Integer,Object>)data.get(i);
            if (element._2 instanceof Types.ByteArraySerializable) {
                buffer.put(i, ((Types.ByteArraySerializable)element._2).toByteArray());
            } else {
                switch (typeInfo.getElementTypeInfo().getPrimitiveType()) {
                    case BYTE: {
                        for (int j = 0; j < data.size(); ++j) {
                            final byte val = (byte)data.get(j)._2;
                            final int offset = j * Types.PrimitiveType.BYTE.size;
                            buffer.getRawData()[offset] = val;
                        }
                    } break;
                    case SHORT: {
                        for (int j = 0; j < data.size(); ++j) {
                            final short val = (short)data.get(j)._2;
                            final int offset = j * Types.PrimitiveType.SHORT.size;
                            buffer.putShort(offset, val);
                        }
                    } break;
                    case INT: {
                        for (int j = 0; j < data.size(); ++j) {
                            final int val = (int)data.get(j)._2;
                            final int offset = j * Types.PrimitiveType.INT.size;
                            buffer.putInt(offset, val);
                        }
                    } break;
                    case LONG: {
                        for (int j = 0; j < data.size(); ++j) {
                            final long val = (long)data.get(j)._2;
                            final int offset = j * Types.PrimitiveType.LONG.size;
                            buffer.putLong(offset, val);
                        }
                    } break;
                    case FLOAT: {
                        for (int j = 0; j < data.size(); ++j) {
                            final float val = (float)data.get(j)._2;
                            final int offset = j * Types.PrimitiveType.FLOAT.size;
                            buffer.putFloat(offset, val);
                        }
                    } break;
                    case DOUBLE: {
                        for (int j = 0; j < data.size(); ++j) {
                            final double val = (double) data.get(j)._2;
                            final int offset = j * Types.PrimitiveType.DOUBLE.size;
                            buffer.putDouble(offset, val);
                        }
                    } break;
                }
            }

            if (hashIndex != null) {
                hashIndex.put(i, element._1);
            }
            else
                arrayIndex[i] = element._1;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int length() { return sparseSize; } /*arrayIndex != null ? arrayIndex.length : hashIndex.length();*/

    @Override
    public Types.TypeInformation getType() { return typeInfo; }

    @Override
    public Types.TypeInformation getElementType() { return typeInfo.getElementTypeInfo(); }

    @Override
    public TypedBuffer getBuffer() { return buffer; }

    @Override
    public byte[] getElement(final int pos) {
        final int elementPos;
        if (arrayIndex != null)
            elementPos = Arrays.binarySearch(arrayIndex, pos);
        else
            elementPos = hashIndex.get(pos);
        if (elementPos >= 0)
            return buffer.extractElementAsByteArray(elementPos);
        else
            return new byte[typeInfo.size()];
    }

    @Override
    public void setElement(final int pos, final byte[] value) { throw new UnsupportedOperationException(); }

    // ---------------------------------------------------

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (!(obj instanceof ImmutableSparseVector)) return false;
        if (length() != ((ImmutableSparseVector) obj).length()) return false;
        return Arrays.equals(((ImmutableSparseVector) obj).buffer.getRawData(), buffer.getRawData());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(buffer.getRawData()); }
}
