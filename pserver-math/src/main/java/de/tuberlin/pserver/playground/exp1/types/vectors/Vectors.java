package de.tuberlin.pserver.playground.exp1.types.vectors;

public final class Vectors {

    // Disallow instantiation.
    private Vectors() {}

    // ---------------------------------------------------

    /*public static class DenseDoubleVector extends DenseVector {

        public DenseDoubleVector(final DenseVectorOld v) { this(v.buffer); }

        public DenseDoubleVector(final double[] data) {
            this(new TypedBuffer(new Types.TypeInformation(Types.PrimitiveType.DOUBLE, data.length)));
            for (int i = 0; i < data.length; ++i)
                buffer.putDouble(i, data[i]);
        }

        public DenseDoubleVector(final List<Double> data) {
            this(new TypedBuffer(new Types.TypeInformation(Types.PrimitiveType.DOUBLE, data.length())));
            for (int i = 0; i < data.length(); ++i)
                buffer.putDouble(i, data.get(i));
        }

        public DenseDoubleVector(final int length) {
            this(new TypedBuffer(new Types.TypeInformation(Types.PrimitiveType.DOUBLE, length)));
        }

        public DenseDoubleVector(final TypedBuffer buffer) {
            super(buffer);
        }

        public double get(final int pos) {
            return UnsafeOp.unsafe.getDouble(buffer.getRawData(), (long)(UnsafeOp.BYTE_ARRAY_OFFSET + pos * Types.DOUBLE_TYPE_INFO.length()));
        }

        public void set(final int pos, final double value) {
            UnsafeOp.unsafe.putDouble(buffer.getRawData(), (long)(UnsafeOp.BYTE_ARRAY_OFFSET + pos * Types.DOUBLE_TYPE_INFO.length()), value);
        }
    }*/

    // ---------------------------------------------------

    /*public static class ImmutableSparseDoubleVector extends ImmutableSparseVector {

        public ImmutableSparseDoubleVector(final int length, final List<Tuple2> data) {
            super(length, new Types.TypeInformation(Types.PrimitiveType.DOUBLE, data.length()), data, false);
        }

        public double get(final int pos) {
            return Types.toDouble(getElement(pos));
        }

        public void set(final int pos, final double value) {
            this.setElement(pos, Types.toByteArray(value));
        }

        @Override
        public Types.TypeInformation getElementType() {
            return null;
        }
    }*/

    // ---------------------------------------------------
    // UDFs.
    // ---------------------------------------------------

    /*public static interface UnaryElementOperation<T> {

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

    public static DenseVector update(final DenseVector v0, final List<Pair<Integer,byte[]>> updates) {
        Preconditions.checkNotNull(updates);
        Preconditions.checkNotNull(v0);
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType) {
            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    for (final Pair<Integer,byte[]> update : updates)
                        v0.data.getRawData()[update.getLeft()] = update.getRight()[0];
                } break;

                case SHORT: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.data.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.data.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                    }
                } break;
                case INT: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.data.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.data.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.data.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.data.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                    }
                } break;
                case LONG: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.data.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.data.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.data.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.data.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                        v0.data.getRawData()[update.getLeft() + 4] = update.getRight()[4];
                        v0.data.getRawData()[update.getLeft() + 5] = update.getRight()[5];
                        v0.data.getRawData()[update.getLeft() + 6] = update.getRight()[6];
                        v0.data.getRawData()[update.getLeft() + 7] = update.getRight()[7];
                    }
                } break;
                case FLOAT: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.data.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.data.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.data.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.data.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                    }
                } break;
                case DOUBLE: {
                    for (final Pair<Integer,byte[]> update : updates) {
                        v0.data.getRawData()[update.getLeft()] = update.getRight()[0];
                        v0.data.getRawData()[update.getLeft() + 1] = update.getRight()[1];
                        v0.data.getRawData()[update.getLeft() + 2] = update.getRight()[2];
                        v0.data.getRawData()[update.getLeft() + 3] = update.getRight()[3];
                        v0.data.getRawData()[update.getLeft() + 4] = update.getRight()[4];
                        v0.data.getRawData()[update.getLeft() + 5] = update.getRight()[5];
                        v0.data.getRawData()[update.getLeft() + 6] = update.getRight()[6];
                        v0.data.getRawData()[update.getLeft() + 7] = update.getRight()[7];
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
        Preconditions.checkNotNull(v0);
        Preconditions.checkNotNull(element);
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType) {
            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    //Preconditions.checkArgument(element.length == Types.PrimitiveType.BYTE.length);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.length;
                        v0.data.getRawData()[offset] = element[0];
                    }
                } break;
                case SHORT: {
                    //Preconditions.checkArgument(element.length == Types.PrimitiveType.SHORT.length);
                    final short val = Types.toShort(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.length;
                        v0.data.putShort(offset, val);
                    }
                } break;
                case INT: {
                    //Preconditions.checkArgument(element.length == Types.PrimitiveType.INT.length);
                    final int val = Types.toInt(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.length;
                        v0.data.putInt(offset, val);
                    }
                } break;
                case LONG: {
                    //Preconditions.checkArgument(element.length == Types.PrimitiveType.LONG.length);
                    final long val = Types.toLong(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.length;
                        v0.data.putLong(offset, val);
                    }
                } break;
                case FLOAT: {
                    //Preconditions.checkArgument(element.length == Types.PrimitiveType.FLOAT.length);
                    final float val = Types.toFloat(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.length;
                        v0.data.putFloat(offset, val);
                    }
                } break;
                case DOUBLE:
                    //Preconditions.checkArgument(element.length == Types.PrimitiveType.DOUBLE.length);
                    final double val = Types.toDouble(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.length;
                        v0.data.putDouble(offset, val);
                    }
                    break;
            }
        } else {
            //Preconditions.checkState(v0.data.length() % element.length == 0);
            for (int i = 0; i < v0.data.length() / element.length; ++i)
                System.arraycopy(element, 0, v0.data.getRawData(), i * element.length, element.length);
        }
        return v0;
    }

    public static DenseVector add(final DenseVector v0, final DenseVector v1) { return add(v0, v1, null); }
    public static DenseVector add(final DenseVector v0, final DenseVector v1, final BinaryElementOperation<Void> add) {
        Preconditions.checkNotNull(v0);
        Preconditions.checkNotNull(v1);
        Preconditions.checkArgument(v0.length() == v1.length());
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType() == v1.typeInfo.getElementTypeInfo().getPrimitiveType()) {

            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.length;
                        v0.data.putShort(offset, (short)(v0.data.getRawData()[offset] + v1.data.getRawData()[offset]));
                    }
                } break;
                case SHORT: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.length;
                        v0.data.putShort(offset, (short)(v0.data.getShort(offset) + v1.data.getShort(offset)));
                    }
                } break;
                case INT: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.length;
                        v0.data.putInt(offset, v0.data.getInt(offset) + v1.data.getInt(offset));
                    }
                } break;
                case LONG: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.length;
                        v0.data.putLong(offset, v0.data.getLong(offset) + v1.data.getLong(offset));
                    }
                } break;
                case FLOAT: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.length;
                        v0.data.putFloat(offset, v0.data.getFloat(offset) + v1.data.getFloat(offset));
                    }
                } break;
                case DOUBLE: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.length;
                        v0.data.putDouble(offset, v0.data.getDouble(offset) + v1.data.getDouble(offset));
                    }
                } break;
                default:
                    throw new IllegalStateException();
            }
        } else {
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                add.apply(v0.typeInfo, i, v0.data.getRawData(),
                        v1.typeInfo, i, v1.data.getRawData());
        }
        return v0;
    }

    public static DenseVector sub(final DenseVector v0, final DenseVector v1) { return sub(v0, v1, null); }
    public static DenseVector sub(final DenseVector v0, final DenseVector v1, final BinaryElementOperation<Void> sub) {
        Preconditions.checkNotNull(v0);
        Preconditions.checkNotNull(v1);
        Preconditions.checkArgument(v0.length() == v1.length());
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType() == v1.typeInfo.getElementTypeInfo().getPrimitiveType()) {

            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.length;
                        v0.data.putShort(offset, (short)(v0.data.getRawData()[offset] - v1.data.getRawData()[offset]));
                    }
                } break;
                case SHORT: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.length;
                        v0.data.putShort(offset, (short)(v0.data.getShort(offset) - v1.data.getShort(offset)));
                    }
                } break;
                case INT: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.length;
                        v0.data.putInt(offset, v0.data.getInt(offset) - v1.data.getInt(offset));
                    }
                } break;
                case LONG: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.length;
                        v0.data.putLong(offset, v0.data.getLong(offset) - v1.data.getLong(offset));
                    }
                } break;
                case FLOAT: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.length;
                        v0.data.putFloat(offset, v0.data.getFloat(offset) - v1.data.getFloat(offset));
                    }
                } break;
                case DOUBLE: {
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.length;
                        v0.data.putDouble(offset, v0.data.getDouble(offset) - v1.data.getDouble(offset));
                    }
                } break;
                default:
                    throw new IllegalStateException();
            }
        } else {
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                sub.apply(v0.typeInfo, i, v0.data.getRawData(),
                        v1.typeInfo, i, v1.data.getRawData());
        }
        return v0;
    }

    public static DenseVector scalarMul(final DenseVector v0, final byte[] element) { return scalarMul(v0, element, null); }
    public static DenseVector scalarMul(final DenseVector v0, final byte[] element, final UnaryElementOperation<Void> mul) {
        Preconditions.checkNotNull(v0);
        Preconditions.checkNotNull(element);
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType) {
            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.BYTE.length);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.BYTE.length;
                        v0.data.getRawData()[offset] = (byte)(v0.data.getRawData()[offset] * element[0]);
                    }
                } break;
                case SHORT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.SHORT.length);
                    final short factor = Types.toShort(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.length;
                        v0.data.putShort(offset, (short)(v0.data.getShort(offset) * factor));
                    }
                } break;
                case INT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.INT.length);
                    final int factor = Types.toInt(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.length;
                        v0.data.putInt(offset, v0.data.getInt(offset) * factor);
                    }
                } break;
                case LONG: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.LONG.length);
                    final long factor = Types.toLong(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.length;
                        v0.data.putLong(offset, v0.data.getLong(offset) * factor);
                    }
                } break;
                case FLOAT: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.FLOAT.length);
                    final float factor = Types.toFloat(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.length;
                        v0.data.putFloat(offset, v0.data.getFloat(offset) * factor);
                    }
                } break;
                case DOUBLE: {
                    Preconditions.checkArgument(element.length == Types.PrimitiveType.DOUBLE.length);
                    final double factor = Types.toDouble(element);
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.DOUBLE.length;
                        v0.data.putDouble(offset, v0.data.getDouble(offset) * factor);
                    }
                } break;
                default:
                    throw new IllegalStateException();
            }
        } else {
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                mul.apply(v0.typeInfo, i, v0.data.getRawData());
        }
        return v0;
    }

    public static byte[] dotProduct(final DenseVector v0, final DenseVector v1) { return dotProduct(v0, v1, null); }
    public static byte[] dotProduct(final DenseVector v0, final DenseVector v1, final BinaryElementOperation<byte[]> dotOp) {
        Preconditions.checkNotNull(v0);
        Preconditions.checkNotNull(v1);
        Preconditions.checkArgument(v0.length() == v1.length());
        if (v0.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType() != null
                && v1.typeInfo.getElementTypeInfo().getPrimitiveType().isNumericType
                && v0.typeInfo.getElementTypeInfo().getPrimitiveType() == v1.typeInfo.getElementTypeInfo().getPrimitiveType()) {

            switch (v0.typeInfo.getElementTypeInfo().getPrimitiveType()) {
                case BYTE: {
                    byte[] result = new byte[Types.PrimitiveType.BYTE.length];
                    for (int i = 0; i < v0.length(); ++i)
                        result[0] += v0.data.getRawData()[i] * v1.data.getRawData()[i];
                    return result;
                }
                case SHORT: {
                    short result = 0;
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.SHORT.length;
                        result += v0.data.getShort(offset) * v1.data.getShort(offset);
                    }
                    return Types.toByteArray(result);
                }
                case INT: {
                    int result = 0;
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.INT.length;
                        result += v0.data.getInt(offset) * v1.data.getInt(offset);
                    }
                    return Types.toByteArray(result);
                }
                case LONG: {
                    long result = 0;
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.length;
                        result += v0.data.getLong(offset) * v1.data.getLong(offset);
                    }
                    return Types.toByteArray(result);
                }
                case FLOAT: {
                    float result = 0;
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.FLOAT.length;
                        result += v0.data.getFloat(offset) * v1.data.getFloat(offset);
                    }
                    return Types.toByteArray(result);
                }
                case DOUBLE: {
                    double result = 0;
                    for (int i = 0; i < v0.length(); ++i) {
                        final int offset = i * Types.PrimitiveType.LONG.length;
                        result += v0.data.getDouble(offset) * v1.data.getDouble(offset);
                    }
                    return Types.toByteArray(result);
                }
                default:
                    throw new IllegalStateException();
            }
        } else {
            byte[] result = null;
            for (int i = 0; i < v0.typeInfo.getNumberOfElements(); ++i)
                result = dotOp.apply(v0.typeInfo, i, v0.data.getRawData(),
                        v1.typeInfo, i, v1.data.getRawData());
            return result;
        }
    }*/
}
