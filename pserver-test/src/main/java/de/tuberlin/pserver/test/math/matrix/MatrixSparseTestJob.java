package de.tuberlin.pserver.test.math.matrix;

import de.tuberlin.pserver.math.exceptions.IncompatibleShapeException;
import de.tuberlin.pserver.math.matrix.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;


public class MatrixSparseTestJob {

    private static final double DELTA = 1e-6;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testCreationWithoutLayout() {
        final Matrix a = new SparseMatrix32F(3, 5);

        assertTrue(a.layout() == Layout.ROW_LAYOUT);
        assertEquals(a.rows(), 3);
        assertEquals(a.cols(), 5);
    }

    @Test
    public void testCreationWithLayout() {
        final Matrix b = new SparseMatrix32F(5, 3, Layout.COLUMN_LAYOUT);

        assertTrue(b.layout() == Layout.COLUMN_LAYOUT);
        assertEquals(b.rows(), 5);
        assertEquals(b.cols(), 3);
    }

    @Test
    public void testCreationCopy() {
        final Matrix a = new SparseMatrix32F(5, 3, Layout.COLUMN_LAYOUT);
        final Matrix b = new SparseMatrix32F((SparseMatrix32F) a);

        assertTrue(b.layout() == Layout.COLUMN_LAYOUT);
        assertEquals(b.rows(), 5);
        assertEquals(b.cols(), 3);
    }

    @Test
    public void testSetAndGet() {
        final Matrix32F a = new SparseMatrix32F(3, 5);

        a.set(1, 2, 1.23f);
        a.set(2, 4, -1.0f);

        assertEquals(a.get(0, 0), 0.0, DELTA);
        assertEquals(a.get(1, 2), 1.23, DELTA);
        assertEquals(a.get(2, 4), -1.0, DELTA);

        assertEquals(a.sizeOf(), 2 * 4);

        a.set(1, 2, 0.0f);
        assertEquals(a.sizeOf(), 1 * 4);

        a.set(0, 0, 0.0f);
        assertEquals(a.sizeOf(), 1 * 4);
    }

    @Test
    public void testSetRowOutOfBounds() {
        final Matrix32F a = new SparseMatrix32F(3, 5);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Row index 3 is out of bounds for Matrix of size(3, 5)");
        a.set(3, 4, 1.0f);
    }

    @Test
    public void testSetColumnOutOfBounds() {
        final Matrix32F a = new SparseMatrix32F(3, 5);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Column index 5 is out of bounds for Matrix of size(3, 5)");
        a.set(2, 5, 1.0f);
    }

    @Test
    public void testCopy() {
        final Matrix32F a = new SparseMatrix32F(3, 5);
        a.set(1, 2, 1.23f);
        a.set(2, 4, -1.0f);

        final Matrix32F b = a.copy();

        assertEquals(b.get(1, 2), 1.23, DELTA);
        assertEquals(b.get(2, 4), -1.0, DELTA);
    }

    @Test
    public void testRowIterator() {
        final Matrix32F a = new SparseMatrix32F(3, 3);
        a.set(1, 1, 1.23f);
        a.set(2, 2, -1.0f);
        a.set(0, 0, 2.0f);
        a.set(0, 2, -4.0f);
        a.set(1, 0, -8.0f);

        final Matrix32F.RowIterator iter = a.rowIterator();

        iter.next();
        Matrix32F firstRow = iter.get();

        assertEquals(firstRow.rows(), 1);
        assertEquals(firstRow.cols(), 3);

        assertEquals(firstRow.get(0), 2.0, DELTA);
        assertEquals(firstRow.get(1), 0.0, DELTA);
        assertEquals(firstRow.get(2), -4.0, DELTA);

        assertTrue(iter.hasNext());

        iter.next();
        Matrix32F secondRow = iter.get();
        assertEquals(secondRow.get(0), -8.0, DELTA);
        assertEquals(secondRow.get(1), 1.23, DELTA);
        assertEquals(secondRow.get(2), 0.0, DELTA);

        assertTrue(iter.hasNext());

        iter.next();
        Matrix32F thirdRow = iter.get();
        assertEquals(thirdRow.get(0), 0.0, DELTA);
        assertEquals(thirdRow.get(1), 0.0, DELTA);
        assertEquals(thirdRow.get(2), -1.0, DELTA);

        assertFalse(iter.hasNext());
    }

    @Test
    public void testDotProduct() {
        final Matrix32F a = new SparseMatrix32F(1, 5);
        a.set(0, 0, 2.0f);
        a.set(0, 2, 11.0f);
        a.set(0, 4, 3.0f);

        final Matrix32F b = new DenseMatrix32F(1, 5);
        b.set(0, 0, -2.0f);
        b.set(0, 4, -3.0f);
        b.set(0, 1, -10f);

        final Matrix32F c = new SparseMatrix32F(1, 5);
        c.set(0, 0, -2.0f);
        c.set(0, 4, -3.0f);
        c.set(0, 1, -10f);

        assertEquals(a.dot(b), -13.0, DELTA);
        assertEquals(a.dot(c), -13.0, DELTA);
        assertEquals(b.dot(a), -13.0, DELTA);
    }

    @Test
    public void testDotProductShapeRow() {
        final Matrix32F a = new SparseMatrix32F(1, 5);
        final Matrix32F b = new SparseMatrix32F(2, 5);

        exception.expect(IncompatibleShapeException.class);
        a.dot(b);
    }

    @Test
    public void testDotProductShapeColumn() {
        final Matrix32F a = new SparseMatrix32F(1, 5);
        final Matrix32F b = new SparseMatrix32F(1, 6);

        exception.expect(IncompatibleShapeException.class);
        a.dot(b);
    }

    @Test
    public void testDotProductLayout() {
        final Matrix32F a = new SparseMatrix32F(1, 5);
        final Matrix32F b = new SparseMatrix32F(1, 5, Layout.COLUMN_LAYOUT);

        exception.expect(IncompatibleShapeException.class);
        a.dot(b);
    }

    @Test
    public void testDotProductNotSingleRow() {
        final Matrix32F a = new SparseMatrix32F(2, 5);

        exception.expect(IncompatibleShapeException.class);
        a.dot(a);
    }

    @Test
    public void testDotProductNotSingleColumn() {
        final Matrix32F a = new SparseMatrix32F(5, 2, Layout.COLUMN_LAYOUT);

        exception.expect(IncompatibleShapeException.class);
        a.dot(a);
    }

    @Test
    public void testGetRow() {
        final Matrix32F a = new SparseMatrix32F(3, 5);
        a.set(0, 0, 2.0f);
        a.set(1, 2, 11.0f);
        a.set(2, 4, -1.0f);

        assertEquals(a.get(0, 0), 2.0, DELTA);
        assertEquals(a.get(1, 2), 11.0, DELTA);
        assertEquals(a.get(2, 4), -1.0, DELTA);

        a.set(1, 4, 42.0f);
        a.set(1, 0, 24.0f);
        a.set(1, 1, -9.0f);

        Matrix32F rowA = a.getRow(1);

        assertEquals(rowA.get(0), 24.0, DELTA);
        assertEquals(rowA.get(1), -9.0, DELTA);
        assertEquals(rowA.get(2), 11.0, DELTA);
        assertEquals(rowA.get(3), 0.0, DELTA);
        assertEquals(rowA.get(4), 42.0, DELTA);

        Matrix32F rowB = a.getRow(1, 1, 2);

        assertEquals(rowB.get(0), -9.0, DELTA);

        exception.expect(IllegalArgumentException.class);
        a.getRow(1, 1, 1);
    }
}