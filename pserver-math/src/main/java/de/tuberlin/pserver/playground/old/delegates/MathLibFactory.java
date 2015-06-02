package de.tuberlin.pserver.playground.old.delegates;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.playground.old.DMatrix;
import de.tuberlin.pserver.playground.old.DVector;
import de.tuberlin.pserver.playground.old.SMatrix;
import de.tuberlin.pserver.playground.old.SVector;
import de.tuberlin.pserver.playground.old.delegates.dense.ejml.EJMLMatrixOps;
import de.tuberlin.pserver.playground.old.delegates.dense.ejml.EJMLVectorOps;
import de.tuberlin.pserver.playground.old.delegates.sparse.ujmp.UJMPMatrixOps;
import de.tuberlin.pserver.playground.old.delegates.sparse.ujmp.UJMPVectorOps;
import org.ujmp.core.matrix.SparseMatrix;

public class MathLibFactory {

    // Disallow instantiation.
    private MathLibFactory() {}

    // ---------------------------------------------------

    public static enum DMathLibrary {

        EJML_LIBRARY,

        MTJ_LIBRARY,

        JBLAS_LIBRARY
    }

    // ---------------------------------------------------

    public static enum SMathLibrary {

        UJMP_LIBRARY
    }

    // ---------------------------------------------------

    public static enum SMatrixEncodingSchemes { // Not used at the moment...

        DOK_ENCODING,

        LIL_ENCODING,

        YALE_ENCODING,

        CRS_ENCODING,

        CDS_ENCODING
    }

    // ---------------------------------------------------

    public static LibraryMatrixOps<DMatrix, DVector> delegateDMatrixOpsTo(final DMathLibrary lib) {
        switch (Preconditions.checkNotNull(lib)) {
            case EJML_LIBRARY:
                return new EJMLMatrixOps();
            case MTJ_LIBRARY:
                throw new UnsupportedOperationException();
            case JBLAS_LIBRARY:
                throw new UnsupportedOperationException();
        }
        throw new IllegalStateException();
    }

    // ---------------------------------------------------

    public static LibraryVectorOps<DVector> delegateDVectorOpsTo(final DMathLibrary lib) {
        switch (Preconditions.checkNotNull(lib)) {
            case EJML_LIBRARY:
                return new EJMLVectorOps();
            case MTJ_LIBRARY:
                throw new UnsupportedOperationException();
            case JBLAS_LIBRARY:
                throw new UnsupportedOperationException();
        }
        throw new IllegalStateException();
    }

    // ---------------------------------------------------

    public static LibraryMatrixOps<SMatrix, SVector> delegateSMatrixOpsTo(final SMathLibrary lib) {
        switch (Preconditions.checkNotNull(lib)) {
            case UJMP_LIBRARY:
                return new UJMPMatrixOps();
        }
        throw new IllegalStateException();
    }

    // ---------------------------------------------------

    public static Object createSMatrixInternalObject(final SMathLibrary lib, final SMatrix matrix) {
        switch (Preconditions.checkNotNull(lib)) {
            case UJMP_LIBRARY:
                return SparseMatrix.factory.zeros(matrix.numRows(), matrix.numCols());
        }
        throw new IllegalStateException();
    }


    // ---------------------------------------------------

    public static LibraryVectorOps<SVector> delegateSVectorOpsTo(final SMathLibrary lib) {
        switch (Preconditions.checkNotNull(lib)) {
            case UJMP_LIBRARY:
                return new UJMPVectorOps();
        }
        throw new IllegalStateException();
    }

    // ---------------------------------------------------

    public static Object createSVectorInternalObject(final SMathLibrary lib, final SVector vector) {
        switch (Preconditions.checkNotNull(lib)) {
            case UJMP_LIBRARY:
                return SparseMatrix.factory.zeros(1, vector.size());
        }
        throw new IllegalStateException();
    }
}
