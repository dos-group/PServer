package de.tuberlin.pserver.math.matrix.sparse;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Random;

/**
 * Created by hegemon on 21.10.15.
 */
public class SparseMatrix64FTest {

  /*private SparseMatrix64F s;

  @Before
  public void setup() {

    Random r = new Random();

    this.s = new SparseMatrix64F(6L, 1L, Layout.ROW_LAYOUT);

    s.set(0L, 0L, r.nextDouble());
    s.set(1L, 0L, r.nextDouble());
    s.set(2L, 0L, r.nextDouble());
    s.set(3L, 0L, r.nextDouble());
    s.set(4L, 0L, r.nextDouble());
    s.set(5L, 0L, r.nextDouble());

  }*/

  /*@Test
  public void testCopy() {

    Sparse64Matrix sCopy = (Sparse64Matrix) this.s.copy();

    Random r = new Random();
    int randomIndex = r.nextInt(6);

    assertEquals("should have same value", s.get(randomIndex), sCopy.get(randomIndex), 0);

  }*/

}
