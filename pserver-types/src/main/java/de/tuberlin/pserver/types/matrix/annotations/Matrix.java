package de.tuberlin.pserver.types.matrix.annotations;


import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

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
}
