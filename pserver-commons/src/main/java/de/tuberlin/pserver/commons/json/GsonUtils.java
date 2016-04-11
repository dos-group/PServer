package de.tuberlin.pserver.commons.json;


import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public final class GsonUtils {

    // Disallow instantiation.
    private GsonUtils() {}

    // ---------------------------------------------------

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public static @interface Exclude {}

    // ---------------------------------------------------

    public static final class AnnotationExclusionStrategy implements ExclusionStrategy {

        @Override
        public boolean shouldSkipField(FieldAttributes f) { return f.getAnnotation(Exclude.class) != null; }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) { return false; }
    }

    // ---------------------------------------------------

    public static Gson createPrettyPrintAndAnnotationExclusionGson() {
        return new GsonBuilder().setPrettyPrinting().setExclusionStrategies(new AnnotationExclusionStrategy()).create();
    }
}
