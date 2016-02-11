package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.collection.CollectionBuilder;
import de.tuberlin.pserver.types.collection.annotation.Collection;
import de.tuberlin.pserver.types.collection.annotation.CollectionDeclaration;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.annotation.MatrixDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TypeFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final List<Class<?>> supportedAnnotations;

    private static final Map<Class<?>, Pair<Class<? extends DistributedDeclaration>, DistributedTypeBuilder>> types;

    static {
        supportedAnnotations = new ArrayList<>();
        supportedAnnotations.add(Matrix.class);
        supportedAnnotations.add(Collection.class);

        types = new HashMap<>();
        types.put(Matrix.class,      Pair.of(MatrixDeclaration.class,        new MatrixBuilder()));
        types.put(Collection.class,  Pair.of(CollectionDeclaration.class,    new CollectionBuilder()));
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    public static boolean isSupported(Class<?> annotationType) {
        return supportedAnnotations.contains(annotationType);
    }

    @SuppressWarnings("unchecked")
    public static <I, T extends DistributedDeclaration, E extends DistributedType> Pair<T,E> createDistributedObject(int nodeID, int[] allNodes, I typeAnnotation) {
        if (!typeAnnotation.getClass().isAnnotation())
            throw new IllegalStateException();
        Pair<Class<? extends DistributedDeclaration>, DistributedTypeBuilder> registeredType
                = types.get(typeAnnotation.getClass());
        if (registeredType == null)
            throw new IllegalStateException();
        Class<? extends DistributedDeclaration> declClass = registeredType.getLeft();
        DistributedDeclaration declaration;
        try {
            declaration = declClass.getDeclaredConstructor(int[].class, typeAnnotation.getClass()).newInstance(allNodes, typeAnnotation);
        } catch (Exception e) { throw new IllegalStateException(e); }
        DistributedTypeBuilder distributedTypeBuilder = registeredType.getRight();
        DistributedType distributedType = distributedTypeBuilder.build(nodeID, declaration);
        return Pair.of((T)declaration, (E)distributedType);
    }
}
