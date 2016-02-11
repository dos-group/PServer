package de.tuberlin.pserver.types.collection.annotation;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.types.DistributedDeclaration;


public class CollectionDeclaration extends DistributedDeclaration {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public CollectionDeclaration(Collection collectionAnnotation) {
        super(ParseUtils.parseNodeRanges(collectionAnnotation.at()), collectionAnnotation.scheme());
    }
}
