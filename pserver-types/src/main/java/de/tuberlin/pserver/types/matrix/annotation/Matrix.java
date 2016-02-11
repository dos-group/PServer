package de.tuberlin.pserver.types.matrix.annotation;


import de.tuberlin.pserver.types.common.FileFormat;
import de.tuberlin.pserver.types.metadata.DistScheme;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(java.lang.annotation.ElementType.FIELD)
public @interface Matrix {

    String at() default "";

    DistScheme scheme() default DistScheme.REPLICATED;

    long rows() default 0;

    long cols() default 0;

    FileFormat format() default FileFormat.UNDEFINED;

    String path() default "";

    String labels() default "";
}
