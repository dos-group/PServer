package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.operations.BinaryOperator;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by hegemon on 21.10.15.
 */
public class SparseMatrix32FTest {

  private SparseMatrix32F sm32f;
  private DenseMatrix32F dm32f;

  @Before
  public void setup() {

    BasicConfigurator.configure();

    Random r = new Random();

    this.sm32f = new SparseMatrix32F(5L, 5L);

    sm32f.set(3L, 0L, r.nextFloat());
    sm32f.set(1L, 1L, r.nextFloat());
    sm32f.set(2L, 2L, r.nextFloat());
    sm32f.set(0L, 3L, r.nextFloat());

    this.dm32f = new DenseMatrix32F(5L, 5L);

    for (int row = 0; row < 5; row++)
      for (int col = 0; col < 5; col++)
        this.dm32f.set(row, col, r.nextFloat());

  }

  @Test
  public void testCopy() {

    SparseMatrix32F mCopy = (SparseMatrix32F) this.sm32f.copy();

    Random r = new Random();
    int randomIndex = r.nextInt(6);

    assertNotEquals("shouldn't be the same object", this.sm32f, mCopy);
    assertEquals("should have same value", this.sm32f.get(randomIndex), mCopy.get(randomIndex), 0);

  }

  @Test
  public void testSetDiagonalsToZero() {

    SparseMatrix32F mDiag = (SparseMatrix32F) this.sm32f.setDiagonalsToZero();

    assertEquals("should be zero", mDiag.get(0, 0), new Float(0));
    assertEquals("should be zero", mDiag.get(1, 1), new Float(0));
    assertEquals("should be zero", mDiag.get(2, 2), new Float(0));

  }

  @Test
  public void testSetArray() {

    SparseMatrix32F mCopy = (SparseMatrix32F) this.sm32f.copy();
    float[] mData = { 0F, 1F, 2F, 3F, 4F, 5F, 6F, 7F, 8F, 9F,
      0F, 1F, 2F, 3F, 4F, 5F, 6F, 7F, 8F, 9F, 0F, 1F, 3F, 4F, 5F };

    mCopy.setArray(mData);

    assertEquals("should be 0.0", mCopy.get(0, 0), new Float(0));
    assertEquals("should be 9.0", mCopy.get(0, 4), new Float(4));

  }

  @Test
  public void testGetRow() {

    int row = 1;
    SparseMatrix32F mRow = (SparseMatrix32F) this.sm32f.getRow(row);

    assertEquals("should be equal", sm32f.get(row, 1), mRow.get(0, 1));
    assertEquals("should be equal", sm32f.get(row, 4), mRow.get(0, 4));

  }

  @Test
  public void testGetCol() {

    int col = 2;
    SparseMatrix32F mCol = (SparseMatrix32F) this.sm32f.getCol(col);

    assertEquals("should be equal", sm32f.get(2, col), mCol.get(2, 0));

  }

  @Test
  public void testApplyOnElements() {

    // Just creates a copy of a global matrix to test for individual test
    SparseMatrix32F mCopy = (SparseMatrix32F) this.sm32f.copy();
    SparseMatrix32F mUnaryOperator, mMatrixElementUnaryOperator, mApplyOnNonZeroElements;

    //------------------------------------------------------------

    // Adds 1 to each element of caller matrix and returns new matrix
    mUnaryOperator = (SparseMatrix32F) mCopy.applyOnElements((v) -> v + 1);

    assertEquals("should be equal", mUnaryOperator.get(0, 1), new Float(mCopy.get(0, 1).floatValue() + 1F));

    // Adds 1 to each element of caller matrix and sets the corresponding element value in passed matrix
    mUnaryOperator = (SparseMatrix32F) mCopy.applyOnElements((v) -> v + 1, mUnaryOperator);

    assertEquals("should be equal", mUnaryOperator.get(0, 2), new Float(mCopy.get(0, 2).floatValue() + 1F));

    //------------------------------------------------------------

    // Runs through each element of caller matrix and adds element of corresponding passed matrix
    // and returns a new matrix
    SparseMatrix32F mBinaryOperator1 = (SparseMatrix32F) mCopy.applyOnElements(mCopy, (left, right) -> left + right);

    assertEquals("should be equal", mBinaryOperator1.get(0, 2), new Float(mCopy.get(0, 2).floatValue() * 2));

    // Runs through each element of caller matrix and adds element of corresponding passed matrix
    // and overwrites another passed matrix then returns it
    SparseMatrix32F mBinaryOperator2 =
            (SparseMatrix32F) mCopy.applyOnElements(mCopy, (left, right) -> left + right, mBinaryOperator1);

    assertEquals("should be equal", mBinaryOperator2.get(0, 2), new Float(mCopy.get(0, 2).floatValue() * 2));
    assertEquals("should be equal", mBinaryOperator1.get(0, 1), mBinaryOperator2.get(0, 1));

    //------------------------------------------------------------

    // Adds 1 to each element of caller matrix and returns new matrix, rows and cols can be modified
    mMatrixElementUnaryOperator = (SparseMatrix32F) mCopy.applyOnElements((rows, cols, v) -> v + 1);

    assertEquals("should be equal", mMatrixElementUnaryOperator.get(2, 0), new Float(mCopy.get(2, 0).floatValue() + 1F));

    // Adds 1 to each element of caller matrix and sets the corresponding element value in passed matrix
    // rows and cols can be modified
    mMatrixElementUnaryOperator = (SparseMatrix32F) mCopy.applyOnElements((rows, cols, v) -> v + 1, mMatrixElementUnaryOperator);

    assertEquals("should be equal", mMatrixElementUnaryOperator.get(2, 1), new Float(mCopy.get(2, 1).floatValue() + 1F));

    //------------------------------------------------------------

    // Adds 1 to each element of caller matrix if it is equal to 0F
    // and returns new matrix, rows and cols can be modified
    mCopy.set(0, 0, 0F);
    mApplyOnNonZeroElements = (SparseMatrix32F) mCopy.applyOnNonZeroElements((row, col, val) -> val + 1);

    assertEquals("should be equal", mApplyOnNonZeroElements.get(0, 0), new Float(0));
    assertEquals("should be equal", mApplyOnNonZeroElements.get(3, 0), new Float(mCopy.get(3, 0).floatValue() + 1F));

  }

  @Test
  public void testAssign() {

    SparseMatrix32F mCopy = (SparseMatrix32F) this.sm32f.copy();

    mCopy.assign(7F);

    assertEquals("should be equal", mCopy.get(0, 2), new Float(7));

    //------------------------------------------------------------

    mCopy.assign(sm32f);

    assertEquals("should be equal", mCopy.get(1, 0), mCopy.get(0, 0));

    //------------------------------------------------------------

    SparseMatrix32F mRow = (SparseMatrix32F) this.sm32f.getRow(1);

    mCopy.assignRow(2, mRow);

    assertEquals("should be equal", mCopy.get(2, 2), mRow.get(0, 2));

    //------------------------------------------------------------

    SparseMatrix32F mCol = (SparseMatrix32F) this.sm32f.getCol(1);

    mCopy.assignColumn(2, mCol);

    assertEquals("should be equal", mCopy.get(2, 2), mCol.get(0, 2));

    //------------------------------------------------------------

    SparseMatrix32F mOffset = (SparseMatrix32F) this.sm32f.copy();

    SparseMatrix32F mSmall = new SparseMatrix32F(2, 2);
    sm32f.set(0L, 0L, 0F);
    sm32f.set(0L, 1L, 0F);
    sm32f.set(1L, 0L, 0F);
    sm32f.set(1L, 1L, 0F);

    mOffset.assign(1, 0, mSmall);

    assertEquals("should be equal", mOffset.get(2, 0), new Float(0));
    assertEquals("should be equal", mOffset.get(2, 1), this.sm32f.get(2, 1));

  }

  @Test
  public void testAggregate() {

    SparseMatrix32F mCopy = (SparseMatrix32F) this.sm32f.copy();

    mCopy.assign(1F);

    assertEquals("should be equal", mCopy.get(2, 2), new Float(1));

    //------------------------------------------------------------

    SparseMatrix32F mAggregate = (SparseMatrix32F) mCopy.aggregateRows((row) -> (float) row.sum());

    assertEquals("should be equal", mAggregate.get(0, 0), new Float(5));

    //------------------------------------------------------------

    assertEquals("should be equal", mCopy.sum(), new Float(25));

  }

  /*@Test
  public void testArithmetic() {

    // TODO: create Unit Test

  }*/

  @Test
  public void testSplicing() {

    SparseMatrix32F subMatrix = (SparseMatrix32F) this.sm32f.subMatrix(1, 0, 2, 3);

    assertEquals("should be equal", subMatrix.rows() * subMatrix.cols(), 6);
    assertEquals("should be equal", subMatrix.get(2), sm32f.get(8));

    //------------------------------------------------------------

    SparseMatrix32F concat = (SparseMatrix32F) this.sm32f.copy().concat(sm32f.getRow(0));

    assertEquals("should be equal", concat.rows(), 6);
    assertEquals("should be equal", concat.get(2, 2), sm32f.get(2, 2));

  }

  /*@Test
  public void testIterator() {

    Matrix.RowIterator rowIterator = this.m.copy().rowIterator();

    System.out.println(this.m);

    System.out.println(rowIterator.get());
    System.out.println(rowIterator.hasNext());

    rowIterator.next();

    System.out.println(rowIterator.get());
    System.out.println(rowIterator.hasNext());

    rowIterator.next();

    System.out.println(rowIterator.get());
    System.out.println(rowIterator.hasNext());

    rowIterator.next();

    System.out.println(rowIterator.get());
    System.out.println(rowIterator.hasNext());


  }*/

}