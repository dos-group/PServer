package de.tuberlin.pserver.types.matrix.annotation;


import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.InputFormat;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;
import de.tuberlin.pserver.types.metadata.DistributionScheme;

public @interface Matrix {

    String at() default "";

    DistributionScheme scheme() default DistributionScheme.REPLICATED;

    long rows() default 0;

    long cols() default 0;

    MatrixType type() default MatrixType.DENSE_FORMAT;

    ElementType elementType() default ElementType.FLOAT_MATRIX;

    InputFormat format() default InputFormat.UNDEFINED;

    String path() default "";

    String labels() default "";
}
