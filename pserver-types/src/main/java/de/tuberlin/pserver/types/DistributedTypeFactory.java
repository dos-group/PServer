package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.collection.CollectionBuilder;
import de.tuberlin.pserver.types.collection.annotation.Collection;
import de.tuberlin.pserver.types.collection.annotation.CollectionDeclaration;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.annotation.Matrix;
import de.tuberlin.pserver.types.matrix.annotation.MatrixDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

public final class DistributedTypeFactory {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Map<Class<?>, Pair<Class<? extends DistributedDeclaration>, DistributedTypeBuilder>> types;

    static {
        types = new HashMap<>();
        types.put(Matrix.class,      Pair.of(MatrixDeclaration.class,        new MatrixBuilder()));
        types.put(Collection.class,  Pair.of(CollectionDeclaration.class,    new CollectionBuilder()));
    }

    // ---------------------------------------------------
    // Factory Method.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public static <I, T extends DistributedDeclaration, E extends DistributedType> Pair<T,E> createDistributedObject(int nodeID, I typeAnnotation) {
        if (!typeAnnotation.getClass().isAnnotation())
            throw new IllegalStateException();
        Pair<Class<? extends DistributedDeclaration>, DistributedTypeBuilder> registeredType
                = types.get(typeAnnotation.getClass());
        if (registeredType == null)
            throw new IllegalStateException();
        Class<? extends DistributedDeclaration> declClass = registeredType.getLeft();
        DistributedDeclaration declaration;
        try {
            declaration = declClass.getDeclaredConstructor(typeAnnotation.getClass()).newInstance(typeAnnotation);
        } catch (Exception e) { throw new IllegalStateException(e); }
        DistributedTypeBuilder distributedTypeBuilder = registeredType.getRight();
        DistributedType distributedType = distributedTypeBuilder.build(nodeID, declaration);
        return Pair.of((T)declaration, (E)distributedType);
    }
}
