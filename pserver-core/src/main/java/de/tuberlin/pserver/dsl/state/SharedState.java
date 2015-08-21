package de.tuberlin.pserver.dsl.state;

import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.types.PartitionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SharedState {;

    LocalScope localScope() default LocalScope.SHARED; // NO SUPPORT FOR LOCAL SCOPES AT THE MOMENT....

    GlobalScope globalScope() default GlobalScope.REPLICATED;

    PartitionType partitionType() default PartitionType.ROW_PARTITIONED;

    long rows() default 0;

    long cols() default 0;

    Layout layout() default Layout.ROW_LAYOUT;

    Format format() default Format.DENSE_FORMAT;

    String path() default "";

    DeltaUpdate delta() default DeltaUpdate.NO_DELTA;
}