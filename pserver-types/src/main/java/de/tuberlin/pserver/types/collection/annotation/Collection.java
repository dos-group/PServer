package de.tuberlin.pserver.types.collection.annotation;


import de.tuberlin.pserver.types.metadata.DistributionScheme;

public @interface Collection {

    String at() default "";

    DistributionScheme scheme() default DistributionScheme.REPLICATED;
}
