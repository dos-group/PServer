package de.tuberlin.pserver.math;

public class TestUtils {

    public static void checkValues(Matrix mat1, Matrix mat2) {
        assert(mat1.numRows() == mat2.numRows());
        assert(mat1.numCols() == mat2.numCols());
        for(long row = 0; row < mat1.numRows(); row++) {
            for(long col = 0; col < mat1.numCols(); col++) {
                assert( Utils.closeTo(mat1.get(row, col), mat2.get(row, col)) );
            }
        }
    }

    public static void checkValues(Matrix mat1, no.uib.cipr.matrix.Matrix mat2) {
        assert(mat1.numRows() == mat2.numRows());
        assert(mat1.numCols() == mat2.numColumns());
        for(long row = 0; row < mat1.numRows(); row++) {
            for(long col = 0; col < mat1.numCols(); col++) {
                assert( Utils.closeTo(mat1.get(row, col), mat2.get(Utils.toInt(row), Utils.toInt(col))) );
            }
        }
    }

}
