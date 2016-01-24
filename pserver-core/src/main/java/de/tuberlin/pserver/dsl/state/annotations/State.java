package de.tuberlin.pserver.dsl.state.annotations;

import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.RowPartitioner;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface State {;

    Scope scope() default Scope.REPLICATED;

    String at() default "";

    Class<? extends MatrixPartitioner> partitioner() default RowPartitioner.class;

    long rows() default 0;

    long cols() default 0;

    FileFormat fileFormat() default FileFormat.UNDEFINED;

    MatrixFormat matrixFormat() default MatrixFormat.DENSE_FORMAT;

    String path() default "";

    String labels() default "";
}