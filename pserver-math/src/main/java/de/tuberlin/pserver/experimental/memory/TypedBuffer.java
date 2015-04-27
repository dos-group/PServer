package de.tuberlin.pserver.experimental.memory;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.experimental.tuples.Tuple;

import java.util.ArrayList;
import java.util.List;

public class TypedBuffer extends Buffer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Types.TypeInformation type;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TypedBuffer(final Types.TypeInformation type) {
        super(type.size());
        this.type = Preconditions.checkNotNull(type);
    }

    public TypedBuffer(final byte[] data, final Types.TypeInformation elementTypeInfo) {
        super(data);
        this.type = new Types.TypeInformation(Preconditions.checkNotNull(elementTypeInfo),
                data.length / elementTypeInfo.size());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Types.TypeInformation getType() { return type; }

    public byte[] extractElementAsByteArray(final int pos) {
        return extractElementsAsByteArray(pos, 1)[0];
    }

    public byte[][] extractElementsAsByteArray(final int pos, final int numElements) {
        Preconditions.checkArgument(pos >= 0 && numElements >= 0);
        final Types.TypeInformation elementTypeInfo = type.getElementTypeInfo();
        final byte[][] range = new byte[numElements][elementTypeInfo.size()];
        final int startOffset = pos * elementTypeInfo.size();
        for (int i = 0; i < numElements; ++i)
            System.arraycopy(getRawData(), startOffset + (i * elementTypeInfo.size()), range[i], 0, elementTypeInfo.size);
        return range;
    }

    public byte[] extractElementSequenceAsByteArray(final int pos, final int numElements) {
        Preconditions.checkArgument(pos >= 0 && numElements >= 0);
        final Types.TypeInformation elementTypeInfo = type.getElementTypeInfo();
        final byte[] elements = new byte[numElements * elementTypeInfo.size()];
        final int startOffset = pos * elementTypeInfo.size();
        System.arraycopy(getRawData(), startOffset, elements, 0, elementTypeInfo.size * numElements);
        return elements;
    }

    public void put(final int pos, final byte[] element) {
        final Types.TypeInformation elementTypeInfo = type.getElementTypeInfo();
        System.arraycopy(element, 0, getRawData(), pos * elementTypeInfo.size(), elementTypeInfo.size());
    }

    public Tuple extractAsTuple(final int elementIndex) {
        Preconditions.checkArgument(elementIndex >= 0);
        Preconditions.checkArgument(type.getElementTypeInfo() != null);
        final Types.TypeInformation elementTypeInfo = type.getElementTypeInfo();
        return Tuple.fromList(extractAsList(getRawData(), elementIndex * elementTypeInfo.size(), elementTypeInfo, new ArrayList<Object>()));
    }

    public static List<Object> extractAsList(final byte[] element, final int elementBaseOffset, final Types.TypeInformation type, final List<Object> fields) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(elementBaseOffset >= 0);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(fields);
        switch (type.getTypeInfoDescriptor()) {
            case TYPE_INFO_PRIMITIVE: {
                switch (type.primitiveType) {
                    case BOOLEAN:
                        throw new UnsupportedOperationException();
                    case CHAR:
                        fields.add(UnsafeOp.unsafe.getChar(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case BYTE:
                        fields.add(UnsafeOp.unsafe.getByte(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case SHORT:
                        fields.add(UnsafeOp.unsafe.getShort(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case INT:
                        fields.add(UnsafeOp.unsafe.getInt(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case LONG:
                        fields.add(UnsafeOp.unsafe.getLong(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case FLOAT:
                        fields.add(UnsafeOp.unsafe.getFloat(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case DOUBLE:
                        fields.add(UnsafeOp.unsafe.getDouble(element, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + elementBaseOffset + type.getBaseOffset())));
                        break;
                }
            } break;
            case TYPE_INFO_COMPOUND: {
                final List<Object> nestedFields = new ArrayList<Object>();
                for (final Types.TypeInformation fieldType : type.getFieldTypes())
                    extractAsList(element, elementBaseOffset, fieldType, nestedFields);
                fields.add(nestedFields);
            } break;

            // -------------------------

            case TYPE_INFO_PRIMITIVE_ARRAY:
                throw new UnsupportedOperationException();
            case TYPE_INFO_COMPOUND_ARRAY:
                throw new UnsupportedOperationException();
        }
        return fields;
    }
}
