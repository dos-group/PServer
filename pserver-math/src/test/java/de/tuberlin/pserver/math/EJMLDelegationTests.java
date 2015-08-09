package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.generators.MatrixGenerator;
import de.tuberlin.pserver.math.generators.VectorGenerator;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.vector.Vector;
//import org.ejml.data.DenseMatrix64F;
//import org.ejml.ops.CommonOps;
import org.junit.Test;

import java.util.Random;

/**
 * Tests if direct calls of EJML produce same result as delegated calls.
 * Errors here indicate, that a delegation is faulty.
 */
public class EJMLDelegationTests {
/*
    @Test
    public void matrixMatrixAdd() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;

            Matrix mat1 = MatrixGenerator.RandomDMatrix(a, b);
            double[] matData1 = mat1.toArray();
            DenseMatrix64F ejmlMat1 = DenseMatrix64F.wrap(a, b, matData1);

            Matrix mat2 = MatrixGenerator.RandomDMatrix(a, b);
            double[] matData2 = mat2.toArray();
            DenseMatrix64F ejmlMat2 = DenseMatrix64F.wrap(a, b, matData2);

            double[] ejmlResData = new double[a*b];
            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(a, b, ejmlResData);
            CommonOps.add(ejmlMat1, ejmlMat2, ejmlRes);

            Matrix res = mat1.add(mat2, mat1);
            double[] resData = res.toArray();

            assert(mat1.toArray() == matData1);
            assert(mat2.toArray() == matData2);
            assert(res.toArray() == matData1 && res.toArray() != matData2);
            assert(java.util.Arrays.equals(resData, ejmlResData));
        }
    }

    @Test
    public void matrixMatrixSub() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;

            Matrix mat1 = MatrixGenerator.RandomDMatrix(a, b);
            double[] matData1 = mat1.toArray();
            DenseMatrix64F ejmlMat1 = DenseMatrix64F.wrap(a, b, matData1);

            Matrix mat2 = MatrixGenerator.RandomDMatrix(a, b);
            double[] matData2 = mat2.toArray();
            DenseMatrix64F ejmlMat2 = DenseMatrix64F.wrap(a, b, matData2);

            double[] ejmlResData = new double[a*b];
            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(a, b, ejmlResData);
            CommonOps.sub(ejmlMat1, ejmlMat2, ejmlRes);

            Matrix res = mat1.sub(mat2, mat1);
            double[] resData = res.toArray();

            assert(mat1.toArray() == matData1);
            assert(mat2.toArray() == matData2);
            assert(res.toArray() == matData1 && res.toArray() != matData2);
            assert(java.util.Arrays.equals(resData, ejmlResData));
        }
    }

    /*@Test
    public void matrixVectorMult() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;

            Vector vec = VectorGenerator.RandomDVector(b);
            double[] vecData = vec.toArray();
            DenseMatrix64F ejmlVec = DenseMatrix64F.wrap(b, 1, vecData);

            Matrix mat = MatrixGenerator.RandomDMatrix(a,b);
            double[] matData = mat.toArray();
            DenseMatrix64F ejmlMat = DenseMatrix64F.wrap(a, b, matData);

            double[] ejmlResData = new double[a];
            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(a, 1, ejmlResData);
            CommonOps.mult(ejmlMat, ejmlVec, ejmlRes);

            Vector res = mat.mul(vec);
            double[] resData = res.toArray();

            assert(mat.toArray() == matData);
            assert(vec.toArray() == vecData);
            assert(resData != vecData);
            assert(java.util.Arrays.equals(resData, ejmlResData));

        }
    }

    @Test
    public void matrixVectorMultResultByReference() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;

            Vector vec = VectorGenerator.RandomDVector(b);
            double[] vecData = vec.toArray();
            DenseMatrix64F ejmlVec = DenseMatrix64F.wrap(b, 1, vecData);

            Matrix mat = MatrixGenerator.RandomDMatrix(a,b);
            double[] matData = mat.toArray();
            DenseMatrix64F ejmlMat = DenseMatrix64F.wrap(a, b, matData);

            Vector res = VectorGenerator.RandomDVector(a);
            double[] resData = res.toArray();
            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(a, 1, resData);

            CommonOps.mult(ejmlMat, ejmlVec, ejmlRes);
            mat.mul(vec, res);

            assert(mat.toArray() == matData);
            assert(vec.toArray() == vecData);
            assert(resData != vecData);
            assert(java.util.Arrays.equals(resData, ejmlRes.getData()));

        }
    }

    @Test
    public void matrixMatrixMult() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;
            int c = (rand.nextInt(10) + 1)*10;

            Matrix mat1 = MatrixGenerator.RandomDMatrix(a,b);
            double[] matData1 = mat1.toArray();
            DenseMatrix64F ejmlMat1 = DenseMatrix64F.wrap(a, b, matData1);

            Matrix mat2 = MatrixGenerator.RandomDMatrix(b,c);
            double[] matData2 = mat2.toArray();
            DenseMatrix64F ejmlMat2 = DenseMatrix64F.wrap(b, c, matData2);

            double[] ejmlResData = new double[a*c];
            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(a, c, ejmlResData);
            CommonOps.mult(ejmlMat1, ejmlMat2, ejmlRes);

            Matrix res = mat1.mul(mat2);
            double[] resData = res.toArray();

            assert(java.util.Arrays.equals(resData, ejmlResData));
        }
    }

    @Test
    public void matrixTranspose() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;
            
            Matrix mat1 = MatrixGenerator.RandomDMatrix(a,b);

            double[] ejmlMat1Data = new double[a*b];
            System.arraycopy(mat1.toArray(), 0, ejmlMat1Data, 0, a*b);
            DenseMatrix64F ejmlMat1 = DenseMatrix64F.wrap(a, b, ejmlMat1Data);
            CommonOps.transpose(ejmlMat1);
            double[] ejmlResData = ejmlMat1.getData();

            Matrix res = mat1.transpose();
            double[] resData = res.toArray();

            assert(java.util.Arrays.equals(resData, ejmlResData));
        }
    }

    @Test
    public void matrixTransposeResultByReference() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;

            Matrix mat1 = MatrixGenerator.RandomDMatrix(a,b);
            Matrix res = new DMatrix(b, a);

            double[] ejmlMat1Data = new double[a*b];
            System.arraycopy(mat1.toArray(), 0, ejmlMat1Data, 0, a*b);
            DenseMatrix64F ejmlMat1 = DenseMatrix64F.wrap(a, b, ejmlMat1Data);

            double[] ejmlResData = new double[a*b];
            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(b, a, ejmlResData);

            CommonOps.transpose(ejmlMat1, ejmlRes);

            Matrix test = mat1.transpose(res);
            assert(test == res);
            double[] resData = res.toArray();

            assert(java.util.Arrays.equals(resData, ejmlResData));
        }
    }

    /*@Test
    public void matrixInvert() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = rand.nextInt(4) + 1;
            int b = a;

            Matrix mat1 = MatrixGenerator.RandomDMatrix(a,b);

            double[] ejmlMat1Data = new double[a*b];
            System.arraycopy(mat1.toArray(), 0, ejmlMat1Data, 0, a*b);
            DenseMatrix64F ejmlMat1 = DenseMatrix64F.wrap(a, b, ejmlMat1Data);

            boolean ejmlRes = CommonOps.invert(ejmlMat1);
            boolean res = mat1.invert();

            assert(ejmlRes == res);
            assert(java.util.Arrays.equals(mat1.toArray(), ejmlMat1Data));
        }
    }

    @Test
    public void matrixScale() {

        Random rand = new Random();

        for(int i=0; i<1000; i++) {
            int a = (rand.nextInt(10) + 1)*10;
            int b = (rand.nextInt(10) + 1)*10;
            double scale = rand.nextDouble();

            Matrix mat = MatrixGenerator.RandomDMatrix(a,b);
            double[] matData = mat.toArray();
            DenseMatrix64F ejmlMat = DenseMatrix64F.wrap(a, b, matData);

            CommonOps.scale(scale, ejmlMat);

            Matrix res = mat.scale(scale, mat);
            assert(res == mat);
            double[] resData = res.toArray();

            assert(resData == matData);
        }
    }*/

//    @Test
//    public void matrixVectorMultBufferOptimization() {
//
//        Random rand = new Random();
//
//        for(int i=0; i<1000; i++) {
//            int a = (rand.nextInt(10) + 1) * 10;
//
//            Vector vec = VectorGenerator.RandomDVector(a);
//            double[] vecData = vec.toArray();
//            DenseMatrix64F ejmlVec = DenseMatrix64F.wrap(a, 1, vecData);
//
//            Matrix mat = MatrixGenerator.RandomDMatrix(a, a);
//            double[] matData = mat.toArray();
//            DenseMatrix64F ejmlMat = DenseMatrix64F.wrap(a, a, matData);
//
//            double[] ejmlResData = new double[a];
//            DenseMatrix64F ejmlRes = DenseMatrix64F.wrap(a, 1, ejmlResData);
//            CommonOps.mult(ejmlMat, ejmlVec, ejmlRes);
//
//            Vector res = mat.mul(vec);
//            double[] resData = res.toArray();
//
//            assert (java.util.Arrays.equals(resData, ejmlResData));
//            assert (resData == vecData);
//
//        }
//    }

}
