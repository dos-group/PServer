package de.tuberlin.pserver.dsl.transaction.annotations;

import de.tuberlin.pserver.dsl.transaction.TransactionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Transaction {

    public String state();

    public TransactionType type();

    public String nodes() default "";
}
