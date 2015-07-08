package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.exceptions.SingularMatrixException;
import de.tuberlin.pserver.math.stuff.VectorFunction;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public interface Matrix extends MObject {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public enum Format {

        SPARSE_MATRIX,

        DENSE_MATRIX
    }

    public enum Layout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Inner Interfaces/Classes.
    // ---------------------------------------------------

    public static interface RowIterator { // ...for ROW_LAYOUT

        public abstract boolean hasNextRow();

        public abstract void nextRow();

        public abstract void nextRandomRow();

        public abstract double getValueOfColumn(final int col);

        public abstract Vector getAsVector();

        public abstract Vector getAsVector(int from, int size);

        public abstract void reset();

        public abstract long numRows();

        public abstract long numCols();
    }

    public static interface ColumnIterator { // ...for COLUMN_LAYOUT

        public abstract boolean hasNextColumn();

        public abstract void nextColumn();

        public abstract double getValueOfRow(final int row);

        public abstract void reset();

        public abstract long numRows();

        public abstract long numCols();
    }

    public static class PartitionShape {

        final long rows;
        final long cols;
        final long rowOffset;
        final long colOffset;

        public PartitionShape(long rows, long cols) {
            this(rows, cols, 0, 0);
        }

        public PartitionShape(long rows, long cols, long rowOffset, long colOffset) {
            this.rows = rows;
            this.cols = cols;
            this.rowOffset = rowOffset;
            this.colOffset = colOffset;
        }

        public long getRows() {
            return rows;
        }

        public long getCols() {
            return cols;
        }

        public long getRowOffset() {
            return rowOffset;
        }

        public long getColOffset() {
            return colOffset;
        }

        public PartitionShape create(long row, long col) {
            return new PartitionShape(row, col);
        }

        public boolean contains(long row, long col) {
            return row < rows && col < cols;
        }
    }

    // ---------------------------------------------------

    public abstract void lock();

    public abstract void unlock();

    public Layout getLayout();

    // ---------------------------------------------------

    public abstract long numRows();

    public abstract long numCols();

    public abstract double get(final long row, final long col);

    public abstract void set(final long row, final long col, final double value);

    public abstract double atomicGet(final long row, final long col);

    public abstract void atomicSet(final long row, final long col, final double value);

    public abstract RowIterator rowIterator();

    public abstract RowIterator rowIterator(final int startRow, final int endRow);

    // ---------------------------------------------------

    public abstract double aggregate(final DoubleBinaryOperator combiner, final DoubleUnaryOperator mapper);

    public abstract Vector aggregateRows(final VectorFunction f);

    public abstract Matrix axpy(final double alpha, final Matrix B);        // A = alpha * B + A

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Addition A += B and returns A. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param B Matrix to add on A
     * @return A after computing A += B
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    public abstract Matrix add(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Addition C = A + B and returns C. <br>
     * <strong>Note: A, B and C have to be of the same shape</strong>
     * @param B Matrix to add on A
     * @param C Matrix to store the result in
     * @return C after computing C = A + B
     * @throws IncompatibleShapeException If the shapes of C, B and A are not equal
     */
    public abstract Matrix add(final Matrix B, final Matrix C);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Subtraction A -= B and returns A. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param B Matrix to subtract from A
     * @return A after computing A -= B
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    public abstract Matrix sub(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Subtraction C = A - B and returns C. <br>
     * <strong>Note: A, B and C have to be of the same shape</strong>
     * @param B Matrix to subtract from A
     * @param C Matrix to store the result in
     * @return C after computing C = A - B
     * @throws IncompatibleShapeException If the shapes of C, B and A are not equal
     */
    public abstract Matrix sub(final Matrix B, final Matrix C);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Multiplication A *= B and returns A. <br>
     * <strong>Note: A is wlog. of shape n x m. B then has to be of shape m x m</strong>
     * @param B Matrix to multiply with A
     * @return A after computing A *= B, or a new Matrix C after computing C = A * B, if the shape of A is not compatible to the resulting shape of A * B
     * @throws IncompatibleShapeException If B is not square with m = A.numCols()
     */
    public abstract Matrix mul(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Multiplication C = A * B and returns C. <br>
     * <strong>Note: A, B and C have to be of shapes n x m, m x o and n x o respectively</strong>
     * @param B Matrix to multiply with A. Of shape m x o
     * @param C Matrix to store the result in. Of shape n x o
     * @return C after computing C = A * B
     * @throws IncompatibleShapeException If A.numCols() != B.numRows() or A.numRows() != C.numRows() or B.numCols() != C.numCols()
     */
    public abstract Matrix mul(final Matrix B, final Matrix C);

    /**
     * Called on Matrix A. Computes Matrix-Vector-Multiplication c = A * b and returns c. <br>
     * <strong>Note: A is wlog. of shape n x m. Vector b has to be of size m and c of size n</strong>
     * @param b Vector to multiply with A
     * @param c Vector to store the result in
     * @return c after computing c = A * b
     * @throws IncompatibleShapeException If b.length() != A.numRows() or c.length() != A.numRows()
     */
    public abstract Vector mul(final Vector b, final Vector c);

    /**
     * Called on Matrix A. Computes Matrix-Scalar-Multiplication A *= a
     * @param a Scalar to multiply with A
     * @return A after computing A *= a
     */
    public abstract Matrix scale(final double a);

    /**
     * Called on Matrix A. Computes Matrix-Scalar-Multiplication B = A * a. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param a Scalar to multiply with A
     * @param B Matrix to store the result in
     * @return B after computing B = A * a
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    public abstract Matrix scale(final double a, final Matrix B);

    /**
     * Called on Matrix A. Computes transpose of A: A = A<sup>T</sup><br>
     * <strong>Note: This method only succeeds for quadratic matrices. For non-quadratic matrices use Matrix.transpose(final Matrix B)</strong>
     * @return A after computing A<sup>T</sup>
     * @throws IncompatibleShapeException If A is not quadratic
     */
    public abstract Matrix transpose();

    /**
     * Called on Matrix A. Computes transpose of A: B = A<sup>T</sup>. <br>
     * <strong>Note: A is wlog. of shape n x m. B has to be of shape m x n</strong>
     * @param B to store the result in
     * @return B after computing B = A<sup>T</sup>
     * @throws IncompatibleShapeException If A.numRows() != B.numCols() or A.numCols() != B.numRows()
     */
    public abstract Matrix transpose(final Matrix B);

    /**
     * Called on Matrix A. Computes inverse of A: A = A<sup>-1</sup>. <br>
     * <strong>Note: This method only succeeds for quadratic matrices. For non-quadratic matrices use Matrix.invert(final Matrix B)</strong>
     * @return A after computing A = A<sup>-1</sup>
     * @throws IllegalStateException If A is singular an its inverse can not be computed
     * @throws IncompatibleShapeException If A is not quadratic
     */
    public abstract Matrix invert();

    /**
     * Called on Matrix A. Computes inverse of A: B = A<sup>-1</sup>. <br>
     * <strong>Note: A is wlog. of shape n x m. B has to be of shape m x n</strong
     * @param B to store the result in
     * @return B after computing B = A<sup>-1</sup>
     * @throws IncompatibleShapeException If A.numRows() != B.numCols() or A.numCols() != B.numRows()
     * @throws SingularMatrixException If A is singular an its inverse can not be computed
     */
    public abstract Matrix invert(final Matrix B);


    /**
     * Called on Matrix A. Computes A = f(A) element-wise.
     * @param f Unary higher order function f: x -> y
     * @return A after computing  A = f(A) element-wise.
     */
    public abstract Matrix applyOnElements(final DoubleUnaryOperator f);

    /**
     * Called on Matrix A. Computes B = f(A) element-wise. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param f Unary higher order function f: x -> y
     * @param B Matrix to store the result in
     * @return B after computing B = f(A) element-wise.
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    public abstract Matrix applyOnElements(final DoubleUnaryOperator f, final Matrix B);

    /**
     * Called on Matrix A. Computes A = f(A, B) element-wise. <br>
     * <strong>Note: A and B are wlog. of shape n x m and o x p respectively. It has to hold that n <= o and m <= p</strong
     * @param f Binary higher order function f: x, y -> z
     * @param B Matrix containing the elements that are used as second arguments in f
     * @return A after computing  A = f(A, B) element-wise.
     * @throws IncompatibleShapeException If the shape of B is smaller than the one of A in any dimension
     */
    public abstract Matrix applyOnElements(final DoubleBinaryOperator f, final Matrix B);

    /**
     * Called on Matrix A. Computes C = f(A, B) element-wise.<br>
     * <strong>Note: A and B are wlog. of shape n x m and o x p respectively. It has to hold that n <= o and m <= p. Also the shape of A an C have to be the same.</strong
     * @param f Binary higher order function f: x, y -> z
     * @param B Matrix containing the elements that are used as second arguments in f
     * @param C to store the result in
     * @return A after computing  C = f(A, B) element-wise.
     * @throws IncompatibleShapeException If the shape of B is smaller than the one of A in any dimension or if the shapes of C and A are not equal
     */
    public abstract Matrix applyOnElements(final DoubleBinaryOperator f, final Matrix B, final Matrix C);

    /**
     * // TODO: @throws and shape requirements
     * Called on Matrix A. Computes a += v for each row-vector in A.
     * @param v Vector to add on each row-vector in A
     * @return A after computing a += v for each row-vector in A.
     */
    public abstract Matrix addVectorToRows(final Vector v);

    /**
     * // TODO: @throws and shape requirements
     * Called on Matrix A. Computes a += v for each row-vector in A.
     * @param v Vector to add on each row-vector in A
     * @param B to store the result in
     * @return A after computing a += v for each row-vector in A.
     */
    public abstract Matrix addVectorToRows(final Vector v, final Matrix B);

    /**
     * // TODO: @throws and shape requirements
     * Called on Matrix A. Computes a += v for each col-vector in A.
     * @param v Vector to add on each col-vector in A
     * @return A after computing a += v for each col-vector in A.
     */
    public abstract Matrix addVectorToCols(final Vector v);

    /**
     * // TODO: @throws and shape requirements
     * Called on Matrix A. Computes a += v for each col-vector in A.
     * @param v Vector to add on each col-vector in A
     * @param B to store the result in
     * @return A after computing a += v for each col-vector in A.
     */
    public abstract Matrix addVectorToCols(final Vector v, final Matrix B);

    /**
     * Called on Matrix A. Sets the diagonal entries of A to zero.
     * @return A after setting its diagonal entries to zero.
     */
    public abstract Matrix setDiagonalsToZero();

    // ---------------------------------------------------

    public Vector rowAsVector();

    public Vector rowAsVector(final long row);

    public Vector rowAsVector(final long row, final long from, final long to);

    public Vector colAsVector();

    public Vector colAsVector(final long col);

    public Vector colAsVector(final long col, final long from, final long to);

    public abstract Matrix assign(final Matrix v);

    public abstract Matrix assign(final double v);

    public abstract Matrix assignRow(final long row, final Vector v);

    public abstract Matrix assignColumn(final long col, final Vector v);

    public abstract Matrix copy();




}
