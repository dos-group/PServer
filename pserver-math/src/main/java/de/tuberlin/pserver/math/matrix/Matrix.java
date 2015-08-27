package de.tuberlin.pserver.math.matrix;

import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.exceptions.SingularMatrixException;
import de.tuberlin.pserver.math.operations.ApplyOnDoubleElements;
import de.tuberlin.pserver.math.utils.VectorFunction;
import de.tuberlin.pserver.math.vector.Vector;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface Matrix extends SharedObject, ApplyOnDoubleElements<Matrix> {

    // ---------------------------------------------------
    // Inner Interfaces/Classes.
    // ---------------------------------------------------

    interface RowIterator { // ...for ROW_LAYOUT

        boolean hasNext();

        void next();

        void nextRandom();

        double value(final long col);

        Vector asVector();

        Vector asVector(int from, int size);

        void reset();

        long rows();

        long cols();

        int rowNum();
    }

    // ---------------------------------------------------

    interface MatrixElementUnaryOperator {

        double apply(long row, long col, double element);
    }

    // ---------------------------------------------------

    public static final class PartitionShape {

        private transient Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

        public final long rows;
        public final long cols;
        public final long rowOffset;
        public final long colOffset;

        public PartitionShape(long rows, long cols) {
            this(rows, cols, 0, 0);
        }

        public PartitionShape(long rows, long cols, long rowOffset, long colOffset) {
            this.rows = rows;
            this.cols = cols;
            this.rowOffset = rowOffset;
            this.colOffset = colOffset;
        }

        public PartitionShape create(long row, long col) {
            return new PartitionShape(row, col);
        }

        public boolean contains(long row, long col) {
            return row < rows && col < cols;
        }

        @Override public String toString() { return "\nPartitionShape " + gson.toJson(this); }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    Layout getLayout();

    // ---------------------------------------------------

    void lock();

    void unlock();

    // ---------------------------------------------------

    long rows();

    long cols();

    // ---------------------------------------------------

    double get(final long index);

    double get(final long row, final long col);

    void set(final long row, final long col, final double value);

    // ---------------------------------------------------

    RowIterator rowIterator();

    RowIterator rowIterator(final int startRow, final int endRow);

    // ---------------------------------------------------

    double aggregate(final DoubleBinaryOperator combiner, final DoubleUnaryOperator mapper);

    Vector aggregateRows(final VectorFunction f);

    // ---------------------------------------------------

    Vector rowAsVector();

    Vector rowAsVector(final long row);

    Vector rowAsVector(final long row, final long from, final long to);

    // ---------------------------------------------------

    Vector colAsVector();

    Vector colAsVector(final long col);

    Vector colAsVector(final long col, final long from, final long to);

    // ---------------------------------------------------

    Matrix assign(final Matrix v);

    Matrix assign(final double v);

    Matrix assignRow(final long row, final Vector v);

    Matrix assignColumn(final long col, final Vector v);

    Matrix assign(final long row, final long col, final Matrix m);

    // ---------------------------------------------------

    Matrix subMatrix(final long row, final long col, final long rowSize, final long colSize);

    // ---------------------------------------------------

    Matrix copy();

    // ---------------------------------------------------

    /**
     * Identical to {@link #add(Matrix, Matrix)} but automatically creates the resulting <code>Matrix C</code>.
     */
    Matrix add(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Addition C = A + B and returns C. <br>
     * <strong>Note: A, B and C have to be of the same shape</strong>
     *
     * @param B Matrix to add on A
     * @param C Matrix to store the result in
     * @return C after computing C = A + B
     * @throws IncompatibleShapeException If the shapes of C, B and A are not equal
     */
    Matrix add(final Matrix B, final Matrix C);

    /**
     * Identical to {@link #sub(Matrix, Matrix)} but automatically creates the resulting <code>Matrix C</code>.
     */
    Matrix sub(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Subtraction C = A - B and returns C. <br>
     * <strong>Note: A, B and C have to be of the same shape</strong>
     *
     * @param B Matrix to subtract from A
     * @param C Matrix to store the result in
     * @return C after computing C = A - B
     * @throws IncompatibleShapeException If the shapes of C, B and A are not equal
     */
    Matrix sub(final Matrix B, final Matrix C);

    /**
     * Identical to {@link #mul(Matrix, Matrix)} but automatically creates the resulting <code>Matrix C</code>.
     */
    Matrix mul(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Multiplication C = A * B and returns C. <br>
     * <strong>Note: A, B and C have to be of shapes n x m, m x o and n x o respectively</strong>
     *
     * @param B Matrix to multiply with A. Of shape m x o
     * @param C Matrix to store the result in. Of shape n x o
     * @return C after computing C = A * B
     * @throws IncompatibleShapeException If A.cols() != B.rows() or A.rows() != C.rows() or B.cols() != C.cols()
     */
    Matrix mul(final Matrix B, final Matrix C);

    /**
     * Called on Matrix A. Computes Matrix-Vector-Multiplication c = A * b and returns c. <br>
     * <strong>Note: A is wlog. of shape n x m. Vector b has to be of size m and c of size n</strong>
     *
     * @param b Vector to multiply with A
     * @param c Vector to store the result in
     * @return c after computing c = A * b
     * @throws IncompatibleShapeException If b.length() != A.rows() or c.length() != A.rows()
     */
    Vector mul(final Vector b, final Vector c);

    /**
     * Identical to {@link #scale(double, Matrix)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix scale(final double a);

    /**
     * Called on Matrix A. Computes Matrix-Scalar-Multiplication B = A * a. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     *
     * @param a Scalar to multiply with A
     * @param B Matrix to store the result in
     * @return B after computing B = A * a
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix scale(final double a, final Matrix B);

    /**
     * Identical to {@link #transpose(Matrix)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix transpose();

    /**
     * Called on Matrix A. Computes transpose of A: B = A<sup>T</sup>. <br>
     * <strong>Note: A is wlog. of shape n x m. B has to be of shape m x n</strong>
     *
     * @param B to store the result in
     * @return B after computing B = A<sup>T</sup>
     * @throws IncompatibleShapeException If A.rows() != B.cols() or A.cols() != B.rows()
     */
    Matrix transpose(final Matrix B);

    /**
     * Identical to {@link #invert(Matrix)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix invert();

    /**
     * Called on Matrix A. Computes inverse of A: B = A<sup>-1</sup>. <br>
     * <strong>Note: A is wlog. of shape n x m. B has to be of shape m x n</strong
     *
     * @param B to store the result in
     * @return B after computing B = A<sup>-1</sup>
     * @throws IncompatibleShapeException If A.rows() != B.cols() or A.cols() != B.rows()
     * @throws SingularMatrixException    If A is singular an its inverse can not be computed
     */
    Matrix invert(final Matrix B);

    /**
     * Identical to {@link #applyOnElements(MatrixElementUnaryOperator)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix applyOnElements(final MatrixElementUnaryOperator f);

    /**
     * Called on Matrix A. Computes B = f(A) element-wise. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     *
     * @param f Unary higher order function f: (row, col, val) -> new_val
     * @param B Matrix to store the result in
     * @return B after computing B = f(A) element-wise.
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix applyOnElements(final MatrixElementUnaryOperator f, final Matrix B);

    /**
     * Identical to {@link #applyOnElements(DoubleUnaryOperator)} but only on non-zero elements.
     */
    Matrix applyOnNonZeroElements(final MatrixElementUnaryOperator f);

    /**
     * Identical to {@link #applyOnElements(DoubleUnaryOperator)} but only on non-zero elements.
     */
    Matrix applyOnNonZeroElements(final MatrixElementUnaryOperator f, Matrix B);

    /**
     * Identical to {@link #addVectorToRows(Vector, Matrix)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix addVectorToRows(final Vector v);

    /**
     * Called on Matrix A. Computes b = a + v for each row-vector a and b in A and B respectively.
     * <strong>Note: It has to hold that A.cols() == v.length(). Also, A and B have to be of the same shape.</strong
     *
     * @param v Vector to add on each row-vector in A
     * @param B to store the result in
     * @return B after computing b = a + v for each row-vector a and b in A and B respectively.
     * @throws IncompatibleShapeException If A.cols() != v.length() or if A and B are of different shapes.
     */
    Matrix addVectorToRows(final Vector v, final Matrix B);

    /**
     * Identical to {@link #addVectorToCols(Vector, Matrix)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix addVectorToCols(final Vector v);

    /**
     * Called on Matrix A. Computes b = a + v for each column-vector a and b in A and B respectively.
     * <strong>Note: It has to hold that A.rows() == v.length(). Also, A and B have to be of the same shape.</strong
     *
     * @param v Vector to add on each col-vector in A
     * @param B to store the result in
     * @return B after computing b = a + v for each column-vector a and b in A and B respectively.
     * @throws IncompatibleShapeException If A.rows() != v.length() or if A and B are of different shapes.
     */
    Matrix addVectorToCols(final Vector v, final Matrix B);

    /**
     * Identical to {@link #setDiagonalsToZero(Matrix)} but automatically creates the resulting <code>Matrix B</code>.
     */
    Matrix setDiagonalsToZero();

    /**
     * Called on Matrix A. Sets B = A with diagonal entries equal to zero.
     * @param B to store the result in
     * @return B after setting B = A with diagonal entries equal to zero.
     */
    Matrix setDiagonalsToZero(Matrix B);
}
