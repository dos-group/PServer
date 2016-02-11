package de.tuberlin.pserver.types.matrix.annotation;


import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.types.DistributedDeclaration;

public final class MatrixDeclaration extends DistributedDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long rows;

    public final long cols;

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
    }
}
