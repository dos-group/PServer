package de.tuberlin.pserver.math;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.memory.TypedBuffer;
import de.tuberlin.pserver.core.memory.Types;
import de.tuberlin.pserver.math.tuples.Tuple;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.math.tuples.Tuple3;
import de.tuberlin.pserver.utils.UnsafeOp;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class DenseVector implements IVector {

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    public static interface UnaryElementOperation<T> {

        public abstract T apply(final Types.TypeInformation e0TypeInfo,
                                final int index0,
                                final byte[] e0);
    }

    public static interface BinaryElementOperation<T> {

        public abstract T apply(final Types.TypeInformation e0TypeInfo,
                                final int index0,
                                final byte[] e0,
                                final Types.TypeInformation e1TypeInfo,
                                final int index1,
                                final byte[] e1);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TypedBuffer buffer;

    private final Types.TypeInformation typeInfo;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseVector(final TypedBuffer buffer) {
        this.buffer = Preconditions.checkNotNull(buffer);
        this.typeInfo = (Types.TypeInformation) buffer.getTypeInfo();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int getLength() { return typeInfo.getNumberOfElements(); }

    public byte[] copyElements(final int elementIdx, final int numElements) {
        Preconditions.checkArgument(elementIdx >= 0 && numElements > 0);
        final int size = numElements * typeInfo.getElementTypeInfo().size();
        final byte[] data = new byte[size];
        System.arraycopy(buffer.getRawData(), elementIdx * typeInfo.getElementTypeInfo().size(), data, 0, size);
        return data;
    }

    // ---------------------------------------------------

    public static DenseVector update(final DenseVector v0, final List<Pair<Integer,byte[]>> updates) {
        Preconditions.checkNotNull(updates);
        Preconditions.checkArgument(v0 != null);
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType) {
            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    for (final Pair<Integer,byte[]> update : updates)
                        v0.buffer.getRawData()[update.getLeft()] = update.getRight()[0];
                } break;

                case SHORT: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.buffer.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.buffer.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                    }
                } break;
                case INT: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.buffer.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.buffer.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.buffer.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.buffer.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                    }
                } break;
                case LONG: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.buffer.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.buffer.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.buffer.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.buffer.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                        v0.buffer.getRawData()[update.getLeft() + 4] = update.getRight()[4];
                        v0.buffer.getRawData()[update.getLeft() + 5] = update.getRight()[5];
                        v0.buffer.getRawData()[update.getLeft() + 6] = update.getRight()[6];
                        v0.buffer.getRawData()[update.getLeft() + 7] = update.getRight()[7];
                    }
                } break;
                case FLOAT: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.buffer.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.buffer.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.buffer.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.buffer.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                    }
                } break;
                case DOUBLE: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.buffer.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.buffer.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.buffer.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.buffer.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                        v0.buffer.getRawData()[update.getLeft() + 4] = update.getRight()[4];
                        v0.buffer.getRawData()[update.getLeft() + 5] = update.getRight()[5];
                        v0.buffer.getRawData()[update.getLeft() + 6] = update.getRight()[6];
                        v0.buffer.getRawData()[update.getLeft() + 7] = update.getRight()[7];
                    }
                } break;
            }
        } else {
            throw new UnsupportedOperationException();
        }
        return v0;
    }

    public static DenseVector fill(final DenseVector v0, final Tuple tuple) { return fill(v0, tuple.toByteArray()); }
    public static DenseVector fill(final DenseVector v0, final byte[] element) {
        Preconditions.checkArgument(v0 != null && element != null);
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType) {
            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.BYTE.size);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.size;
                        v0.buffer.getRawData()[offset] = element[0];
                    }
                } break;
                case SHORT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.SHORT.size);
                    final short val = Types.toShort(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.size;
                        v0.buffer.putShort(offset, val);
                    }
                } break;
                case INT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.INT.size);
                    final int val = Types.toInt(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.size;
                        v0.buffer.putInt(offset, val);
                    }
                } break;
                case LONG: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.LONG.size);
                    final long val = Types.toLong(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.size;
                        v0.buffer.putLong(offset, val);
                    }
                } break;
                case FLOAT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.FLOAT.size);
                    final float val = Types.toFloat(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.size;
                        v0.buffer.putFloat(offset, val);
                    }
                } break;
                case DOUBLE:
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.DOUBLE.size);
                    final double val = Types.toDouble(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.size;
                        v0.buffer.putDouble(offset, val);
                    }
                    break;
            }
        } else {
            Preconditions.checkState(v0.buffer.size() % element.length == 0);
            for (int i = 0; i < v0.buffer.size() / element.length; ++i)
                System.arraycopy(element, 0, v0.buffer.getRawData(), i * element.length, element.length);
        }
        return v0;
    }

    public static DenseVector add(final DenseVector v0, final DenseVector v1) { return add(v0, v1, null); }
    public static DenseVector add(final DenseVector v0, final DenseVector v1, final BinaryElementOperation<Void> add) {
        Preconditions.checkArgument(v0 != null && v1 != null && v0.getLength() == v1.getLength());
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType() == v1.typeInfo.getElementTypeInfo().getPrimitiveType()) {

            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.size;
                        v0.buffer.putShort(offset, (short)(v0.buffer.getRawData()[offset] + v1.buffer.getRawData()[offset]));
                    }
                } break;
                case SHORT: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.size;
                        v0.buffer.putShort(offset, (short)(v0.buffer.getShort(offset) + v1.buffer.getShort(offset)));
                    }
                } break;
                case INT: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.size;
                        v0.buffer.putInt(offset, v0.buffer.getInt(offset) + v1.buffer.getInt(offset));
                    }
                } break;
                case LONG: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.size;
                        v0.buffer.putLong(offset, v0.buffer.getLong(offset) + v1.buffer.getLong(offset));
                    }
                } break;
                case FLOAT: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.size;
                        v0.buffer.putFloat(offset, v0.buffer.getFloat(offset) + v1.buffer.getFloat(offset));
                    }
                } break;
                case DOUBLE: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.size;
                        v0.buffer.putDouble(offset, v0.buffer.getDouble(offset) + v1.buffer.getDouble(offset));
                    }
                } break;
                default:
                    throw new IllegalStateException();
            }
        } else {
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                add.apply(v0.typeInfo, i, v0.buffer.getRawData(),
                          v1.typeInfo, i, v1.buffer.getRawData());
        }
        return v0;
    }

    public static DenseVector sub(final DenseVector v0, final DenseVector v1) { return sub(v0, v1, null); }
    public static DenseVector sub(final DenseVector v0, final DenseVector v1, final BinaryElementOperation<Void> sub) {
        Preconditions.checkArgument(v0 != null && v1 != null && v0.getLength() == v1.getLength());
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType() == v1.typeInfo.getElementTypeInfo().getPrimitiveType()) {

            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.size;
                        v0.buffer.putShort(offset, (short)(v0.buffer.getRawData()[offset] - v1.buffer.getRawData()[offset]));
                    }
                } break;
                case SHORT: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.size;
                        v0.buffer.putShort(offset, (short)(v0.buffer.getShort(offset) - v1.buffer.getShort(offset)));
                    }
                } break;
                case INT: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.size;
                        v0.buffer.putInt(offset, v0.buffer.getInt(offset) - v1.buffer.getInt(offset));
                    }
                } break;
                case LONG: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.size;
                        v0.buffer.putLong(offset, v0.buffer.getLong(offset) - v1.buffer.getLong(offset));
                    }
                } break;
                case FLOAT: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.size;
                        v0.buffer.putFloat(offset, v0.buffer.getFloat(offset) - v1.buffer.getFloat(offset));
                    }
                } break;
                case DOUBLE: {
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.size;
                        v0.buffer.putDouble(offset, v0.buffer.getDouble(offset) - v1.buffer.getDouble(offset));
                    }
                } break;
                default:
                    throw new IllegalStateException();
            }
        } else {
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                sub.apply(v0.typeInfo, i, v0.buffer.getRawData(),
                          v1.typeInfo, i, v1.buffer.getRawData());
        }
        return v0;
    }

    public static DenseVector scalarMul(final DenseVector v0, final byte[] element) { return scalarMul(v0, element, null); }
    public static DenseVector scalarMul(final DenseVector v0, final byte[] element, final UnaryElementOperation<Void> mul) {
        Preconditions.checkArgument(v0 != null);
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType) {
            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.BYTE.size);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.size;
                        v0.buffer.getRawData()[offset] = (byte)(v0.buffer.getRawData()[offset] * element[0]);
                    }
                } break;
                case SHORT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.SHORT.size);
                    final short factor = Types.toShort(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.size;
                        v0.buffer.putShort(offset, (short)(v0.buffer.getShort(offset) * factor));
                    }
                } break;
                case INT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.INT.size);
                    final int factor = Types.toInt(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.size;
                        v0.buffer.putInt(offset, v0.buffer.getInt(offset) * factor);
                    }
                } break;
                case LONG: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.LONG.size);
                    final long factor = Types.toLong(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.size;
                        v0.buffer.putLong(offset, v0.buffer.getLong(offset) * factor);
                    }
                } break;
                case FLOAT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.FLOAT.size);
                    final float factor = Types.toFloat(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.size;
                        v0.buffer.putFloat(offset, v0.buffer.getFloat(offset) * factor);
                    }
                } break;
                case DOUBLE: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.DOUBLE.size);
                    final double factor = Types.toDouble(element);
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.size;
                        v0.buffer.putDouble(offset, v0.buffer.getDouble(offset) * factor);
                    }
                } break;
                default:
                    throw new IllegalStateException();
            }
        } else {
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                mul.apply(v0.typeInfo, i, v0.buffer.getRawData());
        }
        return v0;
    }

    public static byte[] dotProduct(final DenseVector v0, final DenseVector v1) { return dotProduct(v0, v1, null); }
    public static byte[] dotProduct(final DenseVector v0, final DenseVector v1, final BinaryElementOperation<byte[]> dotOp) {
        Preconditions.checkArgument(v0 != null && v1 != null && v0.getLength() == v1.getLength());
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType() == v1.typeInfo.getElementTypeInfo().getPrimitiveType()) {

            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    byte[] result = new byte[Types.PrimitiveType.BYTE.size];
                    for (int i = 0; i < v0.getLength(); ++i)
                        result[0] += v0.buffer.getRawData()[i] * v1.buffer.getRawData()[i];
                    return result;
                }
                case SHORT: {
                    short result = 0;
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.size;
                        result += v0.buffer.getShort(offset) * v1.buffer.getShort(offset);
                    }
                    return Types.toByteArray(result);
                }
                case INT: {
                    int result = 0;
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.size;
                        result += v0.buffer.getInt(offset) * v1.buffer.getInt(offset);
                    }
                    return Types.toByteArray(result);
                }
                case LONG: {
                    long result = 0;
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.size;
                        result += v0.buffer.getLong(offset) * v1.buffer.getLong(offset);
                    }
                    return Types.toByteArray(result);
                }
                case FLOAT: {
                    float result = 0;
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.size;
                        result += v0.buffer.getFloat(offset) * v1.buffer.getFloat(offset);
                    }
                    return Types.toByteArray(result);
                }
                case DOUBLE: {
                    double result = 0;
                    for (int i = 0; i < v0.getLength(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.size;
                        result += v0.buffer.getDouble(offset) * v1.buffer.getDouble(offset);
                    }
                    return Types.toByteArray(result);
                }
                default:
                    throw new IllegalStateException();
            }
        } else {
            byte[] result = null;
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                result = dotOp.apply(v0.typeInfo, i, v0.buffer.getRawData(),
                                     v1.typeInfo, i, v1.buffer.getRawData());
            return result;
        }
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {

        /*final Types.TypeInformation type = Types.TypeBuilder.makeTypeBuilder()
                .add(Types.PrimitiveType.DOUBLE)
                .add(Types.PrimitiveType.INT)
                .add(Types.PrimitiveType.DOUBLE)
                .build();

        final Types.TypeInformation arrayType = new Types.TypeInformation(type, 4);

        final TypedBuffer b0 = new TypedBuffer(arrayType);
        final TypedBuffer b1 = new TypedBuffer(arrayType);
        final DenseVector v0 = new DenseVector(b0);
        final DenseVector v1 = new DenseVector(b1);

        DenseVector.fill(v0, new Tuple3<>(12.12, 12, 34.12));
        DenseVector.fill(v1, new Tuple3<>(1.12, 1, 3.12));
        DenseVector.add(v0, v1, new BinaryElementOperation<Void>() {
            @Override
            public Void apply(Types.TypeInformation e0TypeInfo, int index0, byte[] e0,
                              Types.TypeInformation e1TypeInfo, int index1, byte[] e1) {
                final long i0Offset = e0TypeInfo.getElementFieldOffset(index0, 1);
                final long i1Offset = e1TypeInfo.getElementFieldOffset(index1, 1);
                final int i0 = UnsafeOp.unsafe.getInt(e0, i0Offset);
                final int i1 = UnsafeOp.unsafe.getInt(e1, i1Offset);
                UnsafeOp.unsafe.putInt(e0, i0Offset, i0 + i1);
                return null;
            }
        });

       System.out.println(b0.extractAsTuple(0));*/

        // ---------------------------------------------------

        final Types.TypeInformation type = Types.TypeBuilder.makeTypeBuilder()
                .add(Types.PrimitiveType.DOUBLE)
                .open()
                    .add(Types.PrimitiveType.INT)
                    .add(Types.PrimitiveType.INT)
                .close()
                .add(Types.PrimitiveType.DOUBLE)
                .build();

        final Types.TypeInformation arrayType = new Types.TypeInformation(type, 4);

        final TypedBuffer b0 = new TypedBuffer(arrayType);
        final TypedBuffer b1 = new TypedBuffer(arrayType);
        final DenseVector v0 = new DenseVector(b0);
        final DenseVector v1 = new DenseVector(b1);

        DenseVector.fill(v0, new Tuple3<>(12.12, new Tuple2<>(100, 200), 34.12));
        DenseVector.fill(v1, new Tuple3<>(14.12, new Tuple2<>(100, 200), 23.12));
        DenseVector.add(v0, v1, new BinaryElementOperation<Void>() {
            @Override
            public Void apply(Types.TypeInformation e0TypeInfo, int index0, byte[] e0,
                              Types.TypeInformation e1TypeInfo, int index1, byte[] e1) {

                final long i0Offset = e0TypeInfo.getElementFieldOffset(index0, new int[] {1, 0});
                final long i1Offset = e1TypeInfo.getElementFieldOffset(index1, new int[] {1, 0});
                final int i0 = UnsafeOp.unsafe.getInt(e0, i0Offset);
                final int i1 = UnsafeOp.unsafe.getInt(e1, i1Offset);
                UnsafeOp.unsafe.putInt(e0, i0Offset, i0 + i1);
                return null;
            }
        });

        System.out.println(b0.extractAsTuple(0));
    }
}
