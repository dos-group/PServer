package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.collection.CollectionBuilder;
import de.tuberlin.pserver.types.collection.annotations.Collection;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PServerTypeFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final List<Class<?>> supportedAnnotations;

    private static final Map<Class<?>, AbstractBuilder> registeredTypes;

    static {

        supportedAnnotations = new ArrayList<>();
        supportedAnnotations.add(Matrix.class);
        supportedAnnotations.add(Collection.class);

        registeredTypes = new HashMap<>();
        registeredTypes.put(Matrix.class,      new MatrixBuilder());
        registeredTypes.put(Collection.class,  new CollectionBuilder());
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    public static boolean isSupportedType(Class<?> annotationType) {
        return supportedAnnotations.contains(annotationType);
    }

    @SuppressWarnings("unchecked")
    public static <T extends DistributedTypeInfo> T newInstance(
            int nodeID,
            int[] allNodes,
            Class<?> type,
            String name,
            Annotation typeAnnotation) {

        AbstractBuilder builder = registeredTypes.get(typeAnnotation.annotationType());
        if (builder == null)
            throw new IllegalStateException();
        return (T)builder.build(nodeID, allNodes, type, name, typeAnnotation);
    }
}
