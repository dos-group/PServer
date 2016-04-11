package de.tuberlin.pserver.types.collection.annotations;


import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

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
