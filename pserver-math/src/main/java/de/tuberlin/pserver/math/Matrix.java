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

    enum Format {

        SPARSE_MATRIX,

        DENSE_MATRIX
    }

    enum Layout {

        ROW_LAYOUT,

        COLUMN_LAYOUT
    }

    // ---------------------------------------------------
    // Inner Interfaces/Classes.
    // ---------------------------------------------------

    interface RowIterator { // ...for ROW_LAYOUT

        boolean hasNextRow();

        void nextRow();

        void nextRandomRow();

        double getValueOfColumn(final int col);

        Vector getAsVector();

        Vector getAsVector(int from, int size);

        void reset();

        long numRows();

        long numCols();

        int getCurrentRowNum();
    }

    interface ColumnIterator { // ...for COLUMN_LAYOUT

        boolean hasNextColumn();

        void nextColumn();

        double getValueOfRow(final int row);

        void reset();

        long numRows();

        long numCols();
    }

    interface MatrixElementUnaryOperator {

        double apply(long row, long col, double element);
    }

    class PartitionShape {

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

    void lock();

    void unlock();

    Layout getLayout();

    // ---------------------------------------------------

    long numRows();

    long numCols();

    double get(final long row, final long col);

    void set(final long row, final long col, final double value);

    double atomicGet(final long row, final long col);

    void atomicSet(final long row, final long col, final double value);

    RowIterator rowIterator();

    RowIterator rowIterator(final int startRow, final int endRow);

    // ---------------------------------------------------

    double aggregate(final DoubleBinaryOperator combiner, final DoubleUnaryOperator mapper);

    Vector aggregateRows(final VectorFunction f);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Addition A += B and returns A. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param B Matrix to add on A
     * @return A after computing A += B
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix add(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Addition C = A + B and returns C. <br>
     * <strong>Note: A, B and C have to be of the same shape</strong>
     * @param B Matrix to add on A
     * @param C Matrix to store the result in
     * @return C after computing C = A + B
     * @throws IncompatibleShapeException If the shapes of C, B and A are not equal
     */
    Matrix add(final Matrix B, final Matrix C);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Subtraction A -= B and returns A. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param B Matrix to subtract from A
     * @return A after computing A -= B
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix sub(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Subtraction C = A - B and returns C. <br>
     * <strong>Note: A, B and C have to be of the same shape</strong>
     * @param B Matrix to subtract from A
     * @param C Matrix to store the result in
     * @return C after computing C = A - B
     * @throws IncompatibleShapeException If the shapes of C, B and A are not equal
     */
    Matrix sub(final Matrix B, final Matrix C);

    /**
     * TODO: This only works if B is a lower/left triangular matrix. Do we want do support such rare special cases?
     * Called on Matrix A. Computes Matrix-Matrix-Multiplication A *= B and returns A. <br>
     * <strong>Note: A is wlog. of shape n x m. B then has to be of shape m x m</strong><br>
     * <strong>Note: Also B hast to be a lower/left triangular matrix. This is not checked!</strong>
     * @param B Matrix to multiply with A. B has to be square with m = A.numCols()
     * @return A after computing A *= B
     * @throws IncompatibleShapeException If B is not square with m = A.numCols()
     */
    Matrix mul(final Matrix B);

    /**
     * Called on Matrix A. Computes Matrix-Matrix-Multiplication C = A * B and returns C. <br>
     * <strong>Note: A, B and C have to be of shapes n x m, m x o and n x o respectively</strong>
     * @param B Matrix to multiply with A. Of shape m x o
     * @param C Matrix to store the result in. Of shape n x o
     * @return C after computing C = A * B
     * @throws IncompatibleShapeException If A.numCols() != B.numRows() or A.numRows() != C.numRows() or B.numCols() != C.numCols()
     */
    Matrix mul(final Matrix B, final Matrix C);

    /**
     * Called on Matrix A. Computes Matrix-Vector-Multiplication c = A * b and returns c. <br>
     * <strong>Note: A is wlog. of shape n x m. Vector b has to be of size m and c of size n</strong>
     * @param b Vector to multiply with A
     * @param c Vector to store the result in
     * @return c after computing c = A * b
     * @throws IncompatibleShapeException If b.length() != A.numRows() or c.length() != A.numRows()
     */
    Vector mul(final Vector b, final Vector c);

    /**
     * Called on Matrix A. Computes Matrix-Scalar-Multiplication A *= a
     * @param a Scalar to multiply with A
     * @return A after computing A *= a
     */
    Matrix scale(final double a);

    /**
     * Called on Matrix A. Computes Matrix-Scalar-Multiplication B = A * a. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param a Scalar to multiply with A
     * @param B Matrix to store the result in
     * @return B after computing B = A * a
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix scale(final double a, final Matrix B);

    /**
     * Called on Matrix A. Computes transpose of A: A = A<sup>T</sup><br>
     * <strong>Note: This method only succeeds for quadratic matrices. For non-quadratic matrices use Matrix.transpose(final Matrix B)</strong>
     * @return A after computing A<sup>T</sup>
     * @throws IncompatibleShapeException If A is not quadratic
     */
    Matrix transpose();

    /**
     * Called on Matrix A. Computes transpose of A: B = A<sup>T</sup>. <br>
     * <strong>Note: A is wlog. of shape n x m. B has to be of shape m x n</strong>
     * @param B to store the result in
     * @return B after computing B = A<sup>T</sup>
     * @throws IncompatibleShapeException If A.numRows() != B.numCols() or A.numCols() != B.numRows()
     */
    Matrix transpose(final Matrix B);

    /**
     * Called on Matrix A. Computes inverse of A: A = A<sup>-1</sup>. <br>
     * <strong>Note: This method only succeeds for quadratic matrices. For non-quadratic matrices use Matrix.invert(final Matrix B)</strong>
     * @return A after computing A = A<sup>-1</sup>
     * @throws IllegalStateException If A is singular an its inverse can not be computed
     * @throws IncompatibleShapeException If A is not quadratic
     */
    Matrix invert();

    /**
     * Called on Matrix A. Computes inverse of A: B = A<sup>-1</sup>. <br>
     * <strong>Note: A is wlog. of shape n x m. B has to be of shape m x n</strong
     * @param B to store the result in
     * @return B after computing B = A<sup>-1</sup>
     * @throws IncompatibleShapeException If A.numRows() != B.numCols() or A.numCols() != B.numRows()
     * @throws SingularMatrixException If A is singular an its inverse can not be computed
     */
    Matrix invert(final Matrix B);


    /**
     * Called on Matrix A. Computes A = f(A) element-wise.
     * @param f Unary higher order function f: x -> y
     * @return A after computing  A = f(A) element-wise.
     */
    Matrix applyOnElements(final DoubleUnaryOperator f);

    /**
     * Called on Matrix A. Computes B = f(A) element-wise. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param f Unary higher order function f: x -> y
     * @param B Matrix to store the result in
     * @return B after computing B = f(A) element-wise.
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix applyOnElements(final DoubleUnaryOperator f, final Matrix B);

    /**
     * Called on Matrix A. Computes A = f(A, B) element-wise. <br>
     * <strong>Note: A and B are wlog. of shape n x m and o x p respectively. It has to hold that n <= o and m <= p</strong
     * @param f Binary higher order function f: x, y -> z
     * @param B Matrix containing the elements that are used as second arguments in f
     * @return A after computing  A = f(A, B) element-wise.
     * @throws IncompatibleShapeException If the shape of B is smaller than the one of A in any dimension
     */
    Matrix applyOnElements(final DoubleBinaryOperator f, final Matrix B);

    /**
     * Called on Matrix A. Computes C = f(A, B) element-wise.<br>
     * <strong>Note: A and B are wlog. of shape n x m and o x p respectively. It has to hold that n <= o and m <= p. Also the shape of A an C have to be the same.</strong
     * @param B Matrix containing the elements that are used as second arguments in f
     * @param f Binary higher order function f: x, y -> z
     * @param C to store the result in
     * @return A after computing  C = f(A, B) element-wise.
     * @throws IncompatibleShapeException If the shape of B is smaller than the one of A in any dimension or if the shapes of C and A are not equal
     */
    Matrix applyOnElements(final Matrix B, final DoubleBinaryOperator f, final Matrix C);

    /**
     * Called on Matrix A. Computes A = f(A) element-wise.
     * @param f Unary higher order function f: (row, col, val) -> new_val
     * @return A after computing  A = f(A) element-wise.
     */
    Matrix applyOnElements(final MatrixElementUnaryOperator f);

    /**
     * Called on Matrix A. Computes B = f(A) element-wise. <br>
     * <strong>Note: A and B have to be of the same shape</strong>
     * @param f Unary higher order function f: (row, col, val) -> new_val
     * @param B Matrix to store the result in
     * @return B after computing B = f(A) element-wise.
     * @throws IncompatibleShapeException If the shapes of B and A are not equal
     */
    Matrix applyOnElements(final MatrixElementUnaryOperator f, final Matrix B);

    /**
     * TODO: refactor sparse applyOnElements to implement this functionality
     * Called on Matrix A. Computes A = f(A) element-wise.
     * @param f Unary higher order function f: (row, col, val) -> new_val
     * @return A after computing  A = f(A) element-wise.
     */
    Matrix applyOnNonZeroElements(final MatrixElementUnaryOperator f);

    /**
     * Called on Matrix A. Computes a += v for each row-vector a in A.
     * <strong>Note: It has to hold that A.numCols() == v.length()</strong
     * @param v Vector to add on each row-vector in A
     * @return A after computing a += v for each row-vector in A.
     * @throws IncompatibleShapeException If A.numCols() != v.length()
     */
    Matrix addVectorToRows(final Vector v);

    /**
     * Called on Matrix A. Computes b = a + v for each row-vector a and b in A and B respectively.
     * <strong>Note: It has to hold that A.numCols() == v.length(). Also, A and B have to be of the same shape.</strong
     * @param v Vector to add on each row-vector in A
     * @param B to store the result in
     * @return B after computing b = a + v for each row-vector a and b in A and B respectively.
     * @throws IncompatibleShapeException If A.numCols() != v.length() or if A and B are of different shapes.
     */
    Matrix addVectorToRows(final Vector v, final Matrix B);

    /**
     * Called on Matrix A. Computes a += v for each col-vector in A.
     * <strong>Note: It has to hold that A.numRows() == v.length()</strong
     * @param v Vector to add on each col-vector in A
     * @return A after computing a += v for each col-vector in A.
     * @throws IncompatibleShapeException If A.numRows() != v.length()
     */
    Matrix addVectorToCols(final Vector v);

    /**
     * Called on Matrix A. Computes b = a + v for each column-vector a and b in A and B respectively.
     * <strong>Note: It has to hold that A.numRows() == v.length(). Also, A and B have to be of the same shape.</strong
     * @param v Vector to add on each col-vector in A
     * @param B to store the result in
     * @return B after computing b = a + v for each column-vector a and b in A and B respectively.
     * @throws IncompatibleShapeException If A.numRows() != v.length() or if A and B are of different shapes.
     */
    Matrix addVectorToCols(final Vector v, final Matrix B);

    /**
     * Called on Matrix A. Sets the diagonal entries of A to zero.
     * @return A after setting its diagonal entries to zero.
     */
    Matrix setDiagonalsToZero();

    // ---------------------------------------------------

    Vector rowAsVector();

    Vector rowAsVector(final long row);

    Vector rowAsVector(final long row, final long from, final long to);

    Vector colAsVector();

    Vector colAsVector(final long col);

    Vector colAsVector(final long col, final long from, final long to);

    Matrix assign(final Matrix v);

    Matrix assign(final double v);

    Matrix assignRow(final long row, final Vector v);

    Matrix assignColumn(final long col, final Vector v);

    Matrix copy();

}
