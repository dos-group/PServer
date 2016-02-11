package de.tuberlin.pserver.types.collection.annotation;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.types.metadata.DistributedDeclaration;


public class CollectionDeclaration extends DistributedDeclaration {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public CollectionDeclaration(int[] allNodes, Class<?> type, String name, Collection collectionAnnotation) {
        super("".equals(collectionAnnotation.at()) ? allNodes : ParseUtils.parseNodeRanges(collectionAnnotation.at()),
                collectionAnnotation.scheme(), type, name);
    }
}
