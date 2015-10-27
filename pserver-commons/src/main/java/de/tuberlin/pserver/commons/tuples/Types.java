package de.tuberlin.pserver.commons.tuples;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class Types {

    // Disallow instantiation.
    private Types() {}

    // ---------------------------------------------------

    public static final TypeInformation BYTE_TYPE_INFO      = new TypeInformation(PrimitiveType.BYTE);

    public static final TypeInformation SHORT_TYPE_INFO     = new TypeInformation(PrimitiveType.SHORT);

    public static final TypeInformation INT_TYPE_INFO       = new TypeInformation(PrimitiveType.INT);

    public static final TypeInformation LONG_TYPE_INFO      = new TypeInformation(PrimitiveType.LONG);

    public static final TypeInformation FLOAT_TYPE_INFO     = new TypeInformation(PrimitiveType.FLOAT);

    public static final TypeInformation DOUBLE_TYPE_INFO    = new TypeInformation(PrimitiveType.DOUBLE);

    // ---------------------------------------------------

    public enum PrimitiveType {

        BOOLEAN(1, boolean.class, false), // A serialized boolean is 1 byte!

        CHAR(1, char.class, false),

        BYTE(1, byte.class, true),

        SHORT(2, short.class, true),

        INT(4, int.class, true),

        LONG(8, long.class, true),

        FLOAT(4, float.class, true),

        DOUBLE(8, double.class, true);

        // -----------

        public final boolean isNumericType;

        public final int size;

        public final Class<?> clazz;

        PrimitiveType(final int size,
                      final Class<?> clazz,
                      final boolean isNumericType) {
            Preconditions.checkArgument(size > 0);
            this.size = size;
            this.clazz = Preconditions.checkNotNull(clazz);
            this.isNumericType = isNumericType;
        }
    }

    // ---------------------------------------------------

    public static final class TypeInformation implements Serializable {

        public static enum TypeInformationDescriptor {
            TYPE_INFO_PRIMITIVE,
            TYPE_INFO_PRIMITIVE_ARRAY,
            TYPE_INFO_COMPOUND,
            TYPE_INFO_COMPOUND_ARRAY
        }

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final TypeInformationDescriptor typeInfoDesc;

        public int baseOffset;

        public final int size;

        // ---------------------------------------------------

        public final PrimitiveType primitiveType;

        // ---------------------------------------------------

        public final TypeInformation[] fields;

        public final int[] fieldOffsets;

        // ---------------------------------------------------

        public final TypeInformation arrayElementTypeInfo;

        public final int numElements;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        // primitive type.
        public TypeInformation(final PrimitiveType primitiveType) { this(0, primitiveType); }
        public TypeInformation(final int baseOffset, final PrimitiveType primitiveType) {
            Preconditions.checkArgument(baseOffset >= 0);
            this.typeInfoDesc = TypeInformationDescriptor.TYPE_INFO_PRIMITIVE;
            this.baseOffset = baseOffset;
            this.size = primitiveType.size;
            this.primitiveType = Preconditions.checkNotNull(primitiveType);
            this.fields = null;
            this.fieldOffsets = null;
            this.numElements = 0;
            this.arrayElementTypeInfo = null;
        }

        // primitive array type.
        public TypeInformation(final PrimitiveType primitiveType, final int numElements) { this(0, primitiveType, numElements); }
        public TypeInformation(final int baseOffset, final PrimitiveType primitiveType, final int numElements) {
            Preconditions.checkArgument(baseOffset >= 0);
            this.typeInfoDesc = TypeInformationDescriptor.TYPE_INFO_PRIMITIVE_ARRAY;
            this.baseOffset = baseOffset;
            this.size = numElements * primitiveType.size;
            this.primitiveType = primitiveType;
            this.fields = null;
            this.fieldOffsets = null;
            this.numElements = numElements;
            this.arrayElementTypeInfo = new TypeInformation(baseOffset, Preconditions.checkNotNull(primitiveType));
        }

        // compound type.
        public TypeInformation(final TypeInformation[] fields) { this(0, fields); }
        public TypeInformation(final int baseOffset, final TypeInformation[] fields) {
            Preconditions.checkArgument(baseOffset >= 0);
            Preconditions.checkNotNull(fields);
            Preconditions.checkArgument(fields.length > 0);
            this.typeInfoDesc = TypeInformationDescriptor.TYPE_INFO_COMPOUND;
            this.baseOffset = baseOffset;
            this.primitiveType = null;
            this.fields = fields;
            this.fieldOffsets = new int[fields.length];
            int size = 0, i = 0;
            for (final TypeInformation type : fields) {
                size += type.size();
                fieldOffsets[i++] = baseOffset + size;
            }
            this.size = size;
            this.numElements = 0;
            this.arrayElementTypeInfo = null;
        }

        // compound array type.
        public TypeInformation(final TypeInformation arrayElementTypeInfo, final int numElements) { this(0, arrayElementTypeInfo, numElements); }
        public TypeInformation(final int baseOffset, final TypeInformation arrayElementTypeInfo, final int numElements) {
            Preconditions.checkArgument(baseOffset >= 0);
            Preconditions.checkNotNull(arrayElementTypeInfo);
            Preconditions.checkArgument(numElements > 0);
            this.typeInfoDesc = TypeInformationDescriptor.TYPE_INFO_COMPOUND_ARRAY;
            this.baseOffset = baseOffset;
            this.size = numElements * arrayElementTypeInfo.size();
            this.primitiveType = null;
            this.fields = null;
            this.fieldOffsets = null;
            this.arrayElementTypeInfo = arrayElementTypeInfo;
            this.numElements = numElements;
            this.arrayElementTypeInfo.setBaseOffset(baseOffset);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public TypeInformationDescriptor getTypeInfoDescriptor() { return typeInfoDesc; }

        public int size() {
            return size;
        }

        public int getBaseOffset() {
            return baseOffset;
        }

        // ---------------------------------------------------

        public PrimitiveType getPrimitiveType() {
            return primitiveType;
        }

        // ---------------------------------------------------

        public int getNumberOfFields() {
            return fields != null ? fields.length : 0;
        }

        public TypeInformation getField(int[] selector) {
            if (fields != null) {
                Preconditions.checkNotNull(selector);
                TypeInformation ti = this;
                for (int p : selector)
                    ti = ti.getField(p);
                return ti;
            } else {
                return null;
            }
        }

        public TypeInformation getField(int pos) {
            return fields != null ? fields[pos] : null;
        }

        public int getFieldOffset(int[] selector) {
            if (fields != null) {
                Preconditions.checkNotNull(selector);
                TypeInformation ti = this;
                for (int p : selector)
                    ti = ti.getField(p);
                return ti.getBaseOffset();
            } else {
                return -1;
            }
        }

        public int getFieldOffset(int pos) {
            return getFieldOffset(new int[] { pos });
        }

        public TypeInformation[] getFieldTypes() {
            return fields;
        }

        // ---------------------------------------------------

        public int getNumberOfElements() {
            return numElements;
        }

        public int getElementOffset(final int pos) {
            return arrayElementTypeInfo != null ? baseOffset + (arrayElementTypeInfo.size() * pos) : -1;
        }

        public int getElementFieldOffset(final int elementIndex, final int fieldIndex) {
            return arrayElementTypeInfo != null ? getElementOffset(elementIndex) + arrayElementTypeInfo.getFieldOffset(fieldIndex) + (int)UnsafeOp.BYTE_ARRAY_OFFSET : -1;
        }

        public int getElementFieldOffset(final int elementIndex, final int[] fieldSelector) {
            return arrayElementTypeInfo != null ? getElementOffset(elementIndex) + arrayElementTypeInfo.getFieldOffset(fieldSelector) + (int)UnsafeOp.BYTE_ARRAY_OFFSET : -1;
        }

        public TypeInformation getElementTypeInfo() {
            return arrayElementTypeInfo;
        }

        public void setBaseOffset(final int baseOffset) {
            Preconditions.checkArgument(baseOffset >= 0);
            this.baseOffset = baseOffset;
        }

        // ---------------------------------------------------

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (!(obj instanceof TypeInformation)) return false;
            final TypeInformation rhs = (TypeInformation) obj;
            return new EqualsBuilder()
                    .append(typeInfoDesc, rhs.typeInfoDesc)
                    .append(size, rhs.size)
                    .append(primitiveType, rhs.primitiveType)
                    .append(fields, rhs.fields)
                    .append(fieldOffsets, rhs.fieldOffsets)
                    .append(arrayElementTypeInfo, rhs.arrayElementTypeInfo)
                    .append(numElements, rhs.numElements)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(typeInfoDesc)
                    .append(size)
                    .append(primitiveType)
                    .append(fields)
                    .append(fieldOffsets)
                    .append(arrayElementTypeInfo)
                    .append(numElements)
                    .toHashCode();
        }
    }

    // ---------------------------------------------------

    public static final class TypeBuilder {

        private TypeBuilder parent;

        private List<TypeInformation> types;

        private int offset = 0;

        private int baseOffset = 0;

        public TypeBuilder(final TypeBuilder parent, final int offset) {
            this.parent = parent;
            this.types = new ArrayList<>();
            this.baseOffset = this.offset = offset;
        }

        public TypeBuilder open() {
            final TypeBuilder tib = new TypeBuilder(this, offset);
            return tib;
        }

        public TypeBuilder close() {
            Preconditions.checkState(parent != null);
            parent.add(build(baseOffset));
            return parent;
        }

        public TypeBuilder add(final TypeInformation ti) {
            Preconditions.checkNotNull(ti);
            types.add(ti);
            offset += ti.size();
            return this;
        }

        public TypeBuilder add(final TypeInformation ti, final int numElements) {
            Preconditions.checkNotNull(ti);
            types.add(ti);
            offset += ti.size();
            return this;
        }

        public TypeBuilder add(final PrimitiveType type) {
            Preconditions.checkNotNull(type);
            final TypeInformation pti = new TypeInformation(offset, type);
            types.add(pti);
            offset += pti.size();
            return this;
        }

        public TypeBuilder add(final PrimitiveType primitiveType, final int numElements) {
            Preconditions.checkNotNull(primitiveType);
            Preconditions.checkArgument(numElements > 0);
            final TypeInformation pti = new TypeInformation(offset, primitiveType, numElements);
            types.add(pti);
            offset += pti.size();
            return this;
        }

        public TypeInformation build(int offset) {
            Preconditions.checkState(!types.isEmpty());
            if (types.size() == 1)
                return types.get(0);
            else {
                final TypeInformation[] tia = new TypeInformation[types.size()];
                return new TypeInformation(offset, types.toArray(tia));
            }
        }

        public TypeInformation build() {
            return build(offset);
        }

        public static TypeBuilder makeTypeBuilder() {
            return  new TypeBuilder(null, 0);
        }

        public static TypeBuilder makeTypeBuilder(final int baseOffset) {
            return  new TypeBuilder(null, baseOffset);
        }
    }

    // ---------------------------------------------------

    public static interface ByteArraySerializable {

        public abstract byte[] toByteArray();
    }

    public static boolean toBoolean(final byte[] bytes) {
        return bytes[0] != 0x00;
    }

    public static byte[] toByteArray(final boolean value) {
        return new byte[] {(byte)(value ? 0xFF : 0x00)};
    }

    public static char toChar(final byte[] bytes) {
        return (char)bytes[0];
    }

    public static byte[] toByteArray(final char value) {
        return new byte[] {(byte)(value)};
    }

    public static short toShort(final byte[] bytes) {
        Preconditions.checkArgument(bytes != null && bytes.length == PrimitiveType.SHORT.size);
        return (short)(((bytes[0] & 0xFF) << 8) + (bytes[1] & 0xFF));
    }

    public static byte[] toByteArray(final short value) {
        return new byte[] {
                (byte)(value >>> 8),
                (byte)value
        };
    }

    public static int toInt(final byte[] bytes) {
        Preconditions.checkArgument(bytes != null && bytes.length == PrimitiveType.INT.size);
        return bytes[3] << 24
                | (bytes[2] & 0xFF) << 16
                | (bytes[1] & 0xFF) << 8
                | (bytes[0] & 0xFF);
    }

    public static byte[] toByteArray(final int value) {
        return new byte[] {
                (byte)value,
                (byte)(value >> 8),
                (byte)(value >> 16),
                (byte)(value >> 24)
        };
    }

    public static long toLong(final byte[] bytes) {
        Preconditions.checkArgument(bytes != null && bytes.length == PrimitiveType.LONG.size);
        return    (long)(bytes[7] & 0xFF) << 56
                | (long)(bytes[6] & 0xFF) << 48
                | (long)(bytes[5] & 0xFF) << 40
                | (long)(bytes[4] & 0xFF) << 32
                | (long)(bytes[3] & 0xFF) << 24
                | (long)(bytes[2] & 0xFF) << 16
                | (long)(bytes[1] & 0xFF) << 8
                | (long)(bytes[0] & 0xFF);
    }

    public static byte[] toByteArray(final long value) {
        return new byte[] {
                (byte)value,
                (byte)(value >> 8),
                (byte)(value >> 16),
                (byte)(value >> 24),
                (byte)(value >> 32),
                (byte)(value >> 40),
                (byte)(value >> 48),
                (byte)(value >> 56)
        };
    }

    public static float toFloat(final byte[] bytes) {
        Preconditions.checkArgument(bytes != null && bytes.length == PrimitiveType.FLOAT.size);
        return Float.intBitsToFloat(toInt(bytes));
    }

    public static byte[] toByteArray(final float fValue) {
        return toByteArray(Float.floatToRawIntBits(fValue));
    }

    public static double toDouble(final byte[] bytes) {
        Preconditions.checkArgument(bytes != null && bytes.length == PrimitiveType.DOUBLE.size);
        return Double.longBitsToDouble(toLong(bytes));
    }

    public static byte[] toByteArray(final double value) {
        return toByteArray(Double.doubleToRawLongBits(value));
    }

    // ---------------------------------------------------

    public static byte[] toByteArray(byte[]... values) {
        int size = 0;
        for (final byte[] value : values)
            size += value.length;
        final byte[] dst = new byte[size];
        for (int i = 0, offset = 0; i < values.length; offset += values[i].length, ++i)
            System.arraycopy(values[i], 0, dst, offset, values[i].length);
        return dst;
    }
}