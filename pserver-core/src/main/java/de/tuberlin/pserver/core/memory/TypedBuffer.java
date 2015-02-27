package de.tuberlin.pserver.core.memory;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.tuples.Tuple;
import de.tuberlin.pserver.utils.UnsafeOp;

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

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Types.TypeInformation getTypeInfo() { return type; }

    public byte[] extractAsByteArray(final int pos) {
        return extractAsByteArray(pos, 1)[0];
    }

    public byte[][] extractAsByteArray(final int pos, final int numElements) {
        Preconditions.checkArgument(pos >= 0 && numElements >= 0);
        final byte[][] range = new byte[numElements][type.size()];
        final int startOffset = pos * type.size();
        for (int i = 0; i < numElements; ++i)
            System.arraycopy(getRawData(), startOffset + (i * type.size()), range[i], 0, type.size);
        return range;
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
                        fields.add(UnsafeOp.unsafe.getChar(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case BYTE:
                        fields.add(UnsafeOp.unsafe.getByte(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case SHORT:
                        fields.add(UnsafeOp.unsafe.getShort(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case INT:
                        fields.add(UnsafeOp.unsafe.getInt(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case LONG:
                        fields.add(UnsafeOp.unsafe.getLong(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case FLOAT:
                        fields.add(UnsafeOp.unsafe.getFloat(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
                        break;
                    case DOUBLE:
                        fields.add(UnsafeOp.unsafe.getDouble(element, (UnsafeOp.byteArrayOffset + elementBaseOffset + type.getBaseOffset())));
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
