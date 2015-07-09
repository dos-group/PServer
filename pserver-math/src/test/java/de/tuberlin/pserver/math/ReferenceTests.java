package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.generators.MatrixGenerator;
import de.tuberlin.pserver.math.generators.VectorGenerator;
import org.junit.Test;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import static org.junit.Assert.assertTrue;

/**
 * Tests consistency of object identities. <br>
 * Errors here indicate that wrong objects are returned in Matrix/Vector implementations.
 */
public class ReferenceTests {

    public void testMatrixAddWithoutTargetParameter(final Matrix mat1, final Matrix mat2) {
        final Matrix result = mat1.add(mat2);
        assertTrue("Matrix.add called on object X without target parameter must return X", result == mat1);
    }

    public void testMatrixAddWithTargetParameter(final Matrix mat1, final Matrix mat2, final Matrix target) {
        final Matrix result = mat1.add(mat2, target);
        assertTrue("Matrix.add called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixSubWithoutTargetParameter(final Matrix mat1, final Matrix mat2) {
        final Matrix result = mat1.sub(mat2);
        assertTrue("Matrix.sub called on object X without target parameter must return X", result == mat1);
    }

    public void testMatrixSubWithTargetParameter(final Matrix mat1, final Matrix mat2, final Matrix target) {
        final Matrix result = mat1.sub(mat2, target);
        assertTrue("Matrix.sub called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixMatrixMultWithoutTargetParameter(final Matrix mat1, final Matrix mat2) {
        final Matrix result = mat1.mul(mat2);
        assertTrue("Matrix.mul called on object X without target parameter must return X", result == mat1);
    }

    public void testMatrixMatrixMultWithTargetParameter(final Matrix mat1, final Matrix mat2, final Matrix target) {
        final Matrix result = mat1.mul(mat2, target);
        assertTrue("Matrix.mul called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixVectorMult(final Matrix mat, final Vector vec, final Vector target) {
        final Vector result = mat.mul(vec, target);
        assertTrue("Matrix.mul called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixScaleWithoutTargetParameter(final Matrix mat, double a) {
        final Matrix result = mat.scale(a);
        assertTrue("Matrix.scale called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixScaleWithTargetParameter(final Matrix mat, double a, final Matrix target) {
        final Matrix result = mat.scale(a, target);
        assertTrue("Matrix.scale called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixTransposeWithoutTargetParameter(final Matrix mat) {
        final Matrix result = mat.transpose();
        assertTrue("Matrix.transpose called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixTransposeWithTargetParameter(final Matrix mat, final Matrix target) {
        final Matrix result = mat.transpose(target);
        assertTrue("Matrix.transpose called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixInvertWithoutTargetParameter(final Matrix mat) {
        final Matrix result = mat.invert();
        assertTrue("Matrix.invert called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixInvertWithTargetParameter(final Matrix mat, final Matrix target) {
        final Matrix result = mat.invert(target);
        assertTrue("Matrix.invert called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixUnaryApplyOnElementsWithoutTargetParameter(final Matrix mat) {
        final Matrix result = mat.applyOnElements(DoubleUnaryOperator.identity());
        assertTrue("Matrix.applyOnElements (unary) called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixUnaryApplyOnElementsWithTargetParameter(final Matrix mat, final Matrix target) {
        final Matrix result = mat.applyOnElements(DoubleUnaryOperator.identity(), target);
        assertTrue("Matrix.applyOnElements (unary) called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixBinaryApplyOnElementsWithoutTargetParameter(final Matrix mat, final Matrix B) {
        final Matrix result = mat.applyOnElements((x, y) -> x + y, B);
        assertTrue("Matrix.applyOnElements (binary) called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixBinaryApplyOnElementsWithTargetParameter(final Matrix mat, final Matrix B, final Matrix target) {
        final Matrix result = mat.applyOnElements((x, y) -> x + y, B, target);
        assertTrue("Matrix.applyOnElements (binary) called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixAddVectorToRowsWithoutTargetParameter(final Matrix mat, final Vector b) {
        final Matrix result = mat.addVectorToRows(b);
        assertTrue("Matrix.addVectorToRows called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixAddVectorToRowsWithTargetParameter(final Matrix mat, final Vector b, final Matrix target) {
        final Matrix result = mat.addVectorToRows(b, target);
        assertTrue("Matrix.addVectorToRows called on object X with explicit target parameter T must return T", result == target);
    }

    public void testMatrixAddVectorToColsWithoutTargetParameter(final Matrix mat, final Vector b) {
        final Matrix result = mat.addVectorToCols(b);
        assertTrue("Matrix.addVectorToCols called on object X without target parameter must return X", result == mat);
    }

    public void testMatrixAddVectorToColsWithTargetParameter(final Matrix mat, final Vector b, final Matrix target) {
        final Matrix result = mat.addVectorToCols(b, target);
        assertTrue("Matrix.addVectorToCols called on object X with explicit target parameter T must return T", result == target);
    }

    public void testSetDiagonalsToZero(final Matrix mat) {
        final Matrix result = mat.setDiagonalsToZero();
        assertTrue("Matrix.testsetDiagonalsToZero called on object X must return X", result == mat);
    }

    @Test
    public void testDMatrixReferences() {

        long m = 50;
        long n = 20;
        long o = 30;

        Matrix MN = MatrixGenerator.RandomDMatrix(m, n);
        Matrix NM = MatrixGenerator.RandomDMatrix(n, m);
        Matrix MM = MatrixGenerator.RandomDMatrix(m, m);
        Matrix NO = MatrixGenerator.RandomDMatrix(n, o);
        Matrix MO = MatrixGenerator.RandomDMatrix(m, o);

        Vector M = VectorGenerator.RandomDVector(m);
        Vector N = VectorGenerator.RandomDVector(n);

        testMatrixAddWithoutTargetParameter(MN.copy(), MN.copy());
        testMatrixAddWithTargetParameter(MN.copy(), MN.copy(), MN.copy());

        testMatrixSubWithoutTargetParameter(MN.copy(), MN.copy());
        testMatrixSubWithTargetParameter(MN.copy(), MN.copy(), MN.copy());

        testMatrixMatrixMultWithoutTargetParameter(MM.copy(), MM.copy());
        testMatrixMatrixMultWithTargetParameter(MN.copy(), NO.copy(), MO.copy());

        testMatrixVectorMult(MN.copy(), N.copy(), M.copy());

        testMatrixScaleWithoutTargetParameter(MN.copy(), 1.);
        testMatrixScaleWithTargetParameter(MN.copy(), 1., MN.copy());

        testMatrixTransposeWithoutTargetParameter(MM.copy());
        testMatrixTransposeWithTargetParameter(MN.copy(), NM.copy());

        testMatrixInvertWithoutTargetParameter(MM.copy());
        testMatrixInvertWithTargetParameter(MN.copy(), NM.copy());

        testMatrixUnaryApplyOnElementsWithoutTargetParameter(MN.copy());
        testMatrixUnaryApplyOnElementsWithTargetParameter(MN.copy(), MN.copy());
        testMatrixBinaryApplyOnElementsWithoutTargetParameter(MN.copy(), MN.copy());
        testMatrixBinaryApplyOnElementsWithTargetParameter(MN.copy(), MN.copy(), MN.copy());

        testMatrixAddVectorToRowsWithoutTargetParameter(MN.copy(), N.copy());
        testMatrixAddVectorToRowsWithTargetParameter(MN.copy(), N.copy(), MN.copy());
        testMatrixAddVectorToColsWithoutTargetParameter(MN.copy(), M.copy());
        testMatrixAddVectorToColsWithTargetParameter(MN.copy(), M.copy(), MN.copy());

        testSetDiagonalsToZero(MN.copy());
    }

}
