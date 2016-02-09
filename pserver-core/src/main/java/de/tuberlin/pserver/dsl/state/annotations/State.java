package de.tuberlin.pserver.dsl.state.annotations;

import de.tuberlin.pserver.math.matrix32.partitioner.PartitionerType;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface State {;

    Scope scope() default Scope.REPLICATED;

    String at() default "";

    PartitionerType partitioner() default PartitionerType.ROW_PARTITIONER;

    // --- Matrix Specific ---

    long rows() default 0;

    long cols() default 0;

    MatrixFormat matrixFormat() default MatrixFormat.DENSE_FORMAT;


    FileFormat fileFormat() default FileFormat.UNDEFINED;

    String path() default "";

    String labels() default "";
}