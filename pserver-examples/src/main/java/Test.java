import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.local.LocalCSVInputFile;
import org.apache.commons.csv.CSVRecord;

public class Test {

    /*public static int numOfMachines = 4;

    public static UUID createLocalKey(final int instanceID) {
        int id, i = 0; UUID uid;
        do {
            i++;
            uid = UUID.randomUUID();
            id = (uid.hashCode() & Integer.MAX_VALUE) % numOfMachines;
        } while (id != instanceID);
        System.out.println(i);
        return uid;
    }*/

    public static void main(final String[] args) {
        //System.out.println("=> " + (16000 * 16000 * Types.DOUBLE_TYPE_INFO.size()));


        //System.out.println(createLocalKey(1).toString());
        //System.out.println(createLocalKey(2).toString());
        //System.out.println(createLocalKey(3).toString());

        /*final RowDenseMatrix m = new RowDenseMatrix(100, 600, Types.DOUBLE_TYPE_INFO);
        m.setElement(50, 50, Types.toByteArray(12.4));
        System.out.println(Types.toDouble(m.getElement(50, 50)));
        final DenseVector v = m.getRowAsDenseVector(50);
        System.out.println(Types.toDouble(v.getElement(50)));
        final List<DenseVector> vList = new ArrayList<>();
        for (int i = 0; i < 300; ++i) {
            if (i == 134) {
                final Vectors.DenseDoubleVector dv = Vectors.makeDense(200);
                dv.set(123, 2345.23);
                vList.add(dv);
            } else
                vList.add(new Vectors.DenseDoubleVector(200));
        }
        final RowDenseMatrix m1 = new RowDenseMatrix(vList);
        final DenseVector v1 = m1.getRowAsDenseVector(134);
        System.out.println(Types.toDouble(v1.getElement(123)));
        System.out.println(Types.toDouble(m1.getElement(134, 123)));
        final DenseVector v2 = m1.getColumnAsDenseVector(123);
        System.out.println(Types.toDouble(v2.getElement(134)));
        final Vectors.DenseDoubleVector v3 = new Vectors.DenseDoubleVector(m1.getColumnAsDenseVector(123));
        System.out.println(v3.get(134));
        final Matrices.RowDenseDoubleMatrix m2 = new RowDenseDoubleMatrix(m1);
        System.out.println(m2.get(134, 123));*/


        /*int col_blocks = 3;
        int row_blocks = 3;
        int row_size = 2;
        int col_size = 2;
        long row = 5;
        long col = 5;

        // (c / b) * (a * b) + ((r / a) * (a * b) * d)
        // (c   / b       ) * ( a       * b       ) + ((r   / a       ) * (a        * b       ) * d         )
        // (col / col_size) * (row_size * col_size) + ((row / row_size) * (row_size * col_size) * col_blocks)
        long block_base = (col / col_size) * (row_size * col_size) + ((row / row_size) * (row_size * col_size) * col_blocks);
        long offset = (col % col_size) + ((row % row_size) * col_size) + block_base;

        //long block_base = (row / row_size) * (col_size * row_size) + ((col / col_size) * (col_size * row_size) * row_blocks);
        //long offset = (row % row_size) + ((col % col_size) * row_size) + block_base;
        System.out.println(offset);*/
        /*
        {
            final Types.TypeInformation type = Types.TypeBuilder.makeTypeBuilder()
                    .add(Types.PrimitiveType.DOUBLE)
                    .add(Types.PrimitiveType.INT)
                    .add(Types.PrimitiveType.DOUBLE)
                    .build();

            final Types.TypeInformation arrayType = new Types.TypeInformation(type, 4);

            final TypedBuffer b0 = new TypedBuffer(arrayType);
            final TypedBuffer b1 = new TypedBuffer(arrayType);
            final DenseVector v0 = new DenseVector(b0);
            final DenseVector v1 = new DenseVector(b1);

            Vectors.fill(v0, new Tuple3<>(12.12, 12, 34.12));
            Vectors.fill(v1, new Tuple3<>(1.12, 1, 3.12));
            Vectors.add(v0, v1, new BinaryElementOperation<Void>() {
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

            System.out.println(b0.extractAsTuple(0));
        }

        // ---------------------------------------------------

        {
            final Types.TypeInformation type2 = Types.TypeBuilder.makeTypeBuilder()
                    .add(Types.PrimitiveType.DOUBLE)
                    .open()
                    .add(Types.PrimitiveType.INT)
                    .add(Types.PrimitiveType.INT)
                    .close()
                    .add(Types.PrimitiveType.DOUBLE)
                    .build();

            final Types.TypeInformation arrayType2 = new Types.TypeInformation(type2, 4);

            final TypedBuffer b0 = new TypedBuffer(arrayType2);
            final TypedBuffer b1 = new TypedBuffer(arrayType2);
            final DenseVector v0 = new DenseVector(b0);
            final DenseVector v1 = new DenseVector(b1);

            Vectors.fill(v0, new Tuple3<>(12.12, new Tuple2<>(100, 200), 34.12));
            Vectors.fill(v1, new Tuple3<>(14.12, new Tuple2<>(100, 200), 23.12));
            Vectors.add(v0, v1, new BinaryElementOperation<Void>() {
                @Override
                public Void apply(Types.TypeInformation e0TypeInfo, int index0, byte[] e0,
                                  Types.TypeInformation e1TypeInfo, int index1, byte[] e1) {

                    final long i0Offset = e0TypeInfo.getElementFieldOffset(index0, new int[]{1, 0});
                    final long i1Offset = e1TypeInfo.getElementFieldOffset(index1, new int[]{1, 0});
                    final int i0 = UnsafeOp.unsafe.getInt(e0, i0Offset);
                    final int i1 = UnsafeOp.unsafe.getInt(e1, i1Offset);
                    UnsafeOp.unsafe.putInt(e0, i0Offset, i0 + i1);
                    return null;
                }
            });

            System.out.println(b0.extractAsTuple(0));
        }

        // ---------------------------------------------------

        {
            final Types.TypeInformation type3 = Types.TypeBuilder.makeTypeBuilder()
                    .add(Types.PrimitiveType.DOUBLE)
                    .open()
                    .add(Types.PrimitiveType.INT)
                    .add(Types.PrimitiveType.INT)
                    .close()
                    .add(Types.PrimitiveType.DOUBLE)
                    .build();

            final List<Tuple2<Integer, Object>> elements =
                    Arrays.asList(
                            new Tuple2<Integer, Object>(12344, new Tuple3<>(12.12, new Tuple2<>(100, 200), 34.12)),
                            new Tuple2<Integer, Object>(144, new Tuple3<>(72.12, new Tuple2<>(600, 900), 123.123)),
                            new Tuple2<Integer, Object>(154456, new Tuple3<>(32.12, new Tuple2<>(800, 700), 867.23))
                    );

            final SparseVector v = new SparseVector(160000, type3, elements, false);
            System.out.println(Arrays.equals(v.getElement(144), Tuple.toByteArray(new Tuple3<>(72.12, new Tuple2<>(600, 900), 123.123))));
        }

        System.out.println(true + " " + Types.toBoolean(Types.toByteArray(true)));
        System.out.println(false + " " + Types.toBoolean(Types.toByteArray(false)));
        System.out.println(Character.MAX_VALUE + " " + Types.toChar(Types.toByteArray(Character.MAX_VALUE)));
        System.out.println(Short.MAX_VALUE + " " + Types.toShort(Types.toByteArray(Short.MAX_VALUE)));
        System.out.println(Integer.MAX_VALUE + " " + Types.toInt(Types.toByteArray(Integer.MAX_VALUE)));
        System.out.println(Long.MAX_VALUE + " " + Types.toLong(Types.toByteArray(Long.MAX_VALUE)));
        System.out.println(Float.MAX_VALUE + " " + Types.toFloat(Types.toByteArray(Float.MAX_VALUE)));
        System.out.println(Double.MAX_VALUE + " " + Types.toDouble(Types.toByteArray(Double.MAX_VALUE)));

        final TypeInformation t1 = TypeBuilder.makeTypeBuilder()
                .add(PrimitiveType.INT)
                .add(new TypeInformation(PrimitiveType.BYTE, 20))
                .build();
        final TypeInformation t2 = new TypeInformation(t1, 120);
        final TypedBuffer b1 = new TypedBuffer(t2);

        final byte[] ba = toByteArray(toByteArray(123.213), toByteArray(234), toByteArray(123123L));
        System.out.println(new Buffer(ba).getLong(12));

        TypeInformation ti = TypeBuilder.makeTypeBuilder()
                .add(PrimitiveType.SHORT)
                .add(PrimitiveType.INT)
                .open()
                .add(PrimitiveType.DOUBLE)
                .add(PrimitiveType.LONG)
                .add(PrimitiveType.FLOAT)
                .open()
                .add(PrimitiveType.INT)
                .add(PrimitiveType.INT)
                .close()
                .close()
                .build();

        System.out.println("sizeof(" + ti.toString() + ") = " + ti.size());
        System.out.println("relative offset: " + ti.getFieldOffset(new int[]{2, 3, 1}));


    }*/

        {
            final LocalCSVInputFile fileSection = new LocalCSVInputFile("test.csv", "\n", ',');
            fileSection.computeLocalFileSection(4, 0);
            final FileDataIterator<CSVRecord> fileIterator = fileSection.iterator();

            while (fileIterator.hasNext()) {
                final CSVRecord record = fileIterator.next();
                final StringBuilder strBuilder = new StringBuilder();
                for (int i = 0; i < record.size(); ++i)
                    strBuilder.append(record.get(i)).append(",");
                strBuilder.deleteCharAt(strBuilder.length() - 1);
                System.out.println(strBuilder.toString());
            }
        }

        System.out.println("-------------------------------------------------------------------------");

        {
            final LocalCSVInputFile fileSection = new LocalCSVInputFile("test.csv", "\n", ',');
            fileSection.computeLocalFileSection(4, 1);
            final FileDataIterator<CSVRecord> fileIterator = fileSection.iterator();

            while (fileIterator.hasNext()) {
                final CSVRecord record = fileIterator.next();
                final StringBuilder strBuilder = new StringBuilder();
                for (int i = 0; i < record.size(); ++i)
                    strBuilder.append(record.get(i)).append(",");
                strBuilder.deleteCharAt(strBuilder.length() - 1);
                System.out.println(strBuilder.toString());
            }
        }

        System.out.println("-------------------------------------------------------------------------");

        {
            final LocalCSVInputFile fileSection = new LocalCSVInputFile("test.csv", "\n", ',');
            fileSection.computeLocalFileSection(4, 2);
            final FileDataIterator<CSVRecord> fileIterator = fileSection.iterator();

            while (fileIterator.hasNext()) {
                final CSVRecord record = fileIterator.next();
                final StringBuilder strBuilder = new StringBuilder();
                for (int i = 0; i < record.size(); ++i)
                    strBuilder.append(record.get(i)).append(",");
                strBuilder.deleteCharAt(strBuilder.length() - 1);
                System.out.println(strBuilder.toString());
            }
        }

        System.out.println("-------------------------------------------------------------------------");

        {
            final LocalCSVInputFile fileSection = new LocalCSVInputFile("test.csv", "\n", ',');
            fileSection.computeLocalFileSection(4, 3);
            final FileDataIterator<CSVRecord> fileIterator = fileSection.iterator();

            while (fileIterator.hasNext()) {
                final CSVRecord record = fileIterator.next();
                final StringBuilder strBuilder = new StringBuilder();
                for (int i = 0; i < record.size(); ++i)
                    strBuilder.append(record.get(i)).append(",");
                strBuilder.deleteCharAt(strBuilder.length() - 1);
                System.out.println(strBuilder.toString());
            }
        }

    }
}
