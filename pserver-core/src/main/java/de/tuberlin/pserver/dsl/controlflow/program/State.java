package de.tuberlin.pserver.dsl.controlflow.program;

import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface State {

    // ---------------------------------------------------

    public static final int LOCAL       = 1;

    public static final int GLOBAL      = 2;

    public static final int SHARED      = 4;

    public static final int REPLICATED  = 8;

    public static final int PARTITIONED_INPUT = 16;

    int scope() default REPLICATED;

    // ---------------------------------------------------

    String path() default "";

    // ---------------------------------------------------

    long rows() default 0;

    long cols() default 0;

    Layout layout() default Layout.ROW_LAYOUT;

    Format format() default Format.DENSE_FORMAT;
}