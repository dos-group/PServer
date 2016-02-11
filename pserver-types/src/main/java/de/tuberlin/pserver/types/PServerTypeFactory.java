package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.collection.CollectionBuilder;
import de.tuberlin.pserver.types.collection.annotation.Collection;
import de.tuberlin.pserver.types.collection.annotation.CollectionDeclaration;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.annotation.MatrixDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedType;
import org.apache.commons.lang3.tuple.Pair;

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

    private static final Map<Class<?>, Pair<Class<? extends DistributedDeclaration>, DistributedTypeBuilder>> registeredTypes;

    static {
        supportedAnnotations = new ArrayList<>();
        supportedAnnotations.add(Matrix.class);
        supportedAnnotations.add(Collection.class);

        registeredTypes = new HashMap<>();
        registeredTypes.put(Matrix.class,      Pair.of(MatrixDeclaration.class,        new MatrixBuilder()));
        registeredTypes.put(Collection.class,  Pair.of(CollectionDeclaration.class,    new CollectionBuilder()));
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    public static boolean isSupportedType(Class<?> annotationType) {
        return supportedAnnotations.contains(annotationType);
    }

    @SuppressWarnings("unchecked")
    public static <T extends DistributedDeclaration, E extends DistributedType> Pair<T,E> newInstance(
            int nodeID,
            int[] allNodes,
            Class<?> type,
            String name,
            Annotation typeAnnotation) {

        Pair<Class<? extends DistributedDeclaration>, DistributedTypeBuilder> registeredType
                = registeredTypes.get(typeAnnotation.annotationType());

        if (registeredType == null)
            throw new IllegalStateException();

        Class<? extends DistributedDeclaration> declClass = registeredType.getLeft();
        DistributedDeclaration declaration;
        try {
            declaration = declClass.getDeclaredConstructor(int[].class, type.getClass(), String.class, typeAnnotation.annotationType())
                    .newInstance(allNodes, type, name, typeAnnotation);
        } catch (Exception e) { throw new IllegalStateException(e); }

        DistributedTypeBuilder distributedTypeBuilder = registeredType.getRight();
        DistributedType distributedType = distributedTypeBuilder.build(nodeID, declaration);

        return Pair.of((T)declaration, (E)distributedType);
    }
}
