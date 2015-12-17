package de.tuberlin.pserver.matrix;

import de.tuberlin.pserver.AbstractReplicatedDataType;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.driver.ProgramContext;


public abstract class AbstractReplicatedMatrix<T> extends AbstractReplicatedDataType<T> {

    /**
     * Sole constructor
     *
     * @param crdtId         the ID of the CRDT that this replica belongs to
     * @param noOfReplicas
     * @param programContext the {@code ProgramContext} belonging to this {@code MLProgram}
     */
    protected AbstractReplicatedMatrix(String crdtId, int noOfReplicas, ProgramContext programContext) {
        super(crdtId, noOfReplicas, programContext);
    }

}
