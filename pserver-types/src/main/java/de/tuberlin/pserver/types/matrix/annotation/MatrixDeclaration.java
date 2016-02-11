package de.tuberlin.pserver.types.matrix.annotation;


import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.types.DistributedDeclaration;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.InputFormat;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;

public final class MatrixDeclaration extends DistributedDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long rows;

    public final long cols;

    public final MatrixType type;

    public final ElementType elementType;

    public final InputFormat format;

    public final String path;

    public final String labels;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MatrixDeclaration(Matrix matrixAnnotation) {
        super(ParseUtils.parseNodeRanges(matrixAnnotation.at()), matrixAnnotation.scheme());
        this.rows   = matrixAnnotation.rows();
        this.cols   = matrixAnnotation.cols();
        this.type   = matrixAnnotation.type();
        this.elementType = matrixAnnotation.elementType();
        this.format = matrixAnnotation.format();
        this.path   = matrixAnnotation.path();
        this.labels = matrixAnnotation.labels();
    }
}
