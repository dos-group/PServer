package de.tuberlin.pserver.dsl.state.annotations;

import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.runtime.filesystem.Format;
import de.tuberlin.pserver.runtime.state.partitioner.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.state.partitioner.RowPartitioner;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface State {;

    Scope scope() default Scope.REPLICATED;

    String at() default "";

    Class<? extends IMatrixPartitioner> partitioner() default RowPartitioner.class;

    long rows() default 0;

    long cols() default 0;

    Format format() default Format.DENSE_FORMAT;

    String path() default "";
}