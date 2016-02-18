package de.tuberlin.pserver.types.typeinfo.annotations;

import de.tuberlin.pserver.types.typeinfo.properties.FileFormat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Load {

    FileFormat fileFormat() default FileFormat.SVM_FORMAT;

    String filePath();

    String labels() default "";
}
