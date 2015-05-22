package de.tuberlin.pserver.math;

import de.tuberlin.pserver.math.generators.MatrixGenerator;
import de.tuberlin.pserver.math.generators.VectorGenerator;
import org.junit.Test;

/**
 * Tests consistency of object identites.
 * Errors here indicate that wrong objects are returned in Matrix/Vector implementations.
 */
public class ReferenceTests {

    public void testReferencesMatrixOps(Matrix mat1, Matrix mat2, Vector vec) {

        // addition does not change object identity
        assert(mat1.add(mat2) == mat1);
        // addition does not buffer identity
        assert(mat1.add(mat2).toArray() == mat1.toArray());

        // subtraction does not change object identity
        assert(mat1.sub(mat2) == mat1);
        // subtraction does not buffer identity
        assert(mat1.sub(mat2).toArray() == mat1.toArray());

        // mul mat x vec does change object identity
        assert(mat1.mul(vec) != mat1);
        // mul mat x vec does change buffer identity
        assert(mat1.mul(vec).toArray() != mat1.toArray());

        // mul mat x mat does change object identity
        assert(mat1.mul(mat2) != mat1);
        // mul mat x mat does buffer identity
        assert(mat1.mul(mat2).toArray() != mat1.toArray());

        // scale does not change object identity
        assert(mat1.scale(1.0) == mat1);
        // scale does not change buffer identity
        assert(mat1.scale(1.0).toArray() == mat1.toArray());
    }

    @Test
    public void testReferences() {

        // test DMatrix
        testReferencesMatrixOps(
                MatrixGenerator.RandomDMatrix(50,50),
                MatrixGenerator.RandomDMatrix(50,50),
                VectorGenerator.RandomDVector(50)
        );

    }

}
