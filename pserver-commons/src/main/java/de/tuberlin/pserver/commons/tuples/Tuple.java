package de.tuberlin.pserver.commons.tuples;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.unsafe.UnsafeOp;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;


public abstract class Tuple
        implements Serializable, Iterable<Object>, Comparable<Tuple>, Types.ByteArraySerializable {

    private static final long serialVersionUID = -1L;

    public abstract <T> T getField(final int pos);

    public abstract <T> void setField(final T value, final int pos);

    public abstract int length();

    public abstract Iterator<Object> iterator();

    public abstract int compareTo(final Tuple o);

    @Override
    public byte[] toByteArray() {
        return toByteArray(this);
    }

    public static Tuple createTuple(int num) {
        switch(num) {
            case 1:
                return new Tuple1<>();
            case 2:
                return new Tuple2<>();
            case 3:
                return new Tuple3<>();
            case 4:
                return new Tuple4<>();
            case 5:
                return new Tuple5<>();
            case 6:
                return new Tuple6<>();
            case 7:
                return new Tuple7<>();
            default:
                throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    public static Tuple fromList(final List<Object> elements) {
        final Tuple at = createTuple(elements.size());
        for (int i = 0; i < elements.size(); ++i) {
            final Object o = elements.get(i);
            if (o instanceof List)
                at.setField(fromList((List) o), i);
            else
                at.setField(o, i);
        }
        return at;
    }

    public static byte[] toByteArray(final Tuple tuple) {
        final byte[] ba = new byte[getSize(Preconditions.checkNotNull(tuple))];
        final int offset = toByteArray0(ba, 0, tuple);
        Preconditions.checkState(ba.length == offset);
        return ba;
    }

    private static int getSize(final Tuple tuple) {
        int size = 0;
        for (final Object o : tuple) {
            if (o instanceof Tuple) {
                size += getSize((Tuple)o);
            } else {
                if (o instanceof Boolean)
                    size += Types.PrimitiveType.BOOLEAN.size;
                else if (o instanceof Character)
                    size += Types.PrimitiveType.CHAR.size;
                else if (o instanceof Byte)
                    size += Types.PrimitiveType.BYTE.size;
                else if (o instanceof Short)
                    size += Types.PrimitiveType.SHORT.size;
                else if (o instanceof Integer)
                    size += Types.PrimitiveType.INT.size;
                else if (o instanceof Long)
                    size += Types.PrimitiveType.LONG.size;
                else if (o instanceof Float)
                    size += Types.PrimitiveType.FLOAT.size;
                else if (o instanceof Double)
                    size += Types.PrimitiveType.DOUBLE.size;
                else
                    throw new IllegalStateException();
            }
        }
        return size;
    }

    @SuppressWarnings("unchecked")
    private static int toByteArray0(final byte[] ba, int offset, final Tuple tuple) {
        final int oldOffset = offset;
        for (final Object o : tuple) {
            if (o instanceof Tuple) {
                offset += toByteArray0(ba, offset, (Tuple)o);
            } else {
                if (o instanceof Boolean) {
                    UnsafeOp.unsafe.putBoolean(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Boolean) o);
                    offset += Types.PrimitiveType.BOOLEAN.size;
                } else if (o instanceof Character) {
                    UnsafeOp.unsafe.putChar(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Character) o);
                    offset += Types.PrimitiveType.CHAR.size;
                } else if (o instanceof Byte) {
                    UnsafeOp.unsafe.putByte(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Byte) o);
                    offset += Types.PrimitiveType.BYTE.size;
                } else if (o instanceof Short) {
                    UnsafeOp.unsafe.putShort(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Short) o);
                    offset += Types.PrimitiveType.SHORT.size;
                } else if (o instanceof Integer) {
                    UnsafeOp.unsafe.putInt(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Integer) o);
                    offset += Types.PrimitiveType.INT.size;
                } else if (o instanceof Long) {
                    UnsafeOp.unsafe.putLong(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Long) o);
                    offset += Types.PrimitiveType.LONG.size;
                } else if (o instanceof Float) {
                    UnsafeOp.unsafe.putFloat(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Float) o);
                    offset += Types.PrimitiveType.FLOAT.size;
                } else if (o instanceof Double) {
                    UnsafeOp.unsafe.putDouble(ba, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + offset), (Double) o);
                    offset += Types.PrimitiveType.DOUBLE.size;
                } else
                    throw new IllegalStateException();
            }
        }
        return offset - oldOffset;
    }
}
