package de.tuberlin.pserver.types.matrix.annotation;


import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.types.DistributedDeclaration;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;

public final class MatrixDeclaration extends DistributedDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long rows;

    public final long cols;

    public final MatrixType mtxType;

    public final ElementType elementType;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixDeclaration(int[] allNodes, Class<?> type, String name, Matrix matrixAnnotation) {
        super("".equals(matrixAnnotation.at()) ? allNodes : ParseUtils.parseNodeRanges(matrixAnnotation.at()),
                matrixAnnotation.scheme(),
                type,
                name,
                matrixAnnotation.format(),
                matrixAnnotation.path(),
                matrixAnnotation.labels()
        );

        this.rows   = matrixAnnotation.rows();
        this.cols   = matrixAnnotation.cols();
        this.mtxType = matrixAnnotation.type();
        this.elementType = matrixAnnotation.elementType();
    }
}
