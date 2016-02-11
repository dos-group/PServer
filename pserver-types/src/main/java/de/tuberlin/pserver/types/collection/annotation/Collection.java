package de.tuberlin.pserver.types.collection.annotation;


import de.tuberlin.pserver.types.metadata.DistScheme;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Collection {

    String at() default "";

    DistScheme scheme() default DistScheme.REPLICATED;
}
