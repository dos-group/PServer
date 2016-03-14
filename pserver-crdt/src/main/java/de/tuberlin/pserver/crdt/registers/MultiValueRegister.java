package de.tuberlin.pserver.crdt.registers;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;


public class MultiValueRegister<T> extends AbstractRegister<Set<T>> implements Register<Set<T>> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // Value mapped to a version vector
    private final Map<T,int[]> register;

    // Current local version vector
    private int[] versionVector;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MultiValueRegister(String id, int noOfReplicas, ProgramContext programContext) {

        super(id, noOfReplicas, programContext);

        register = new HashMap<>();

        versionVector = new int[noOfReplicas];

        Arrays.fill(versionVector, 0);

        ready();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized boolean set(Set<T> elements) {

        Preconditions.checkState(!isFinished, "After finish() has been called on a CRDT no more changes can be made to it");

        int[] version = increaseVersionVector();

        register.clear();

        for(T element : elements) {

            register.put(element, version);

            broadcast(new TaggedOperation<>(Operation.OpType.ASSIGN, elements, version));

        }

        return true;

    }

    @Override
    public synchronized Set<T> get() {

        return register.keySet();

    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    @Override
    protected boolean update(int srcNodeId, Operation op) {

        @SuppressWarnings("unchecked")
        TaggedOperation<Set<T>,int[]> taggedOp = (TaggedOperation<Set<T>,int[]>) op;

        switch(taggedOp.getType()) {

            case ASSIGN:

                merge(taggedOp.getValue(), taggedOp.getTag());

                return true;

            default:

                throw new IllegalArgumentException("MultiValueRegister CRDTs do not allow the " + op.getType() + " operation.");

        }

    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized void merge(Set<T> elements, int[] version) {

        updateVersionVector(version);

        if(isDominated(version)) return;

        for(T element : elements) {

            register.put(element, version);

        }

    }

    private synchronized void updateVersionVector(int[] version) {

        for(int i = 0; i < versionVector.length; i++) {

            if(versionVector[i] < version[i]) versionVector[i] = version[i];

        }

    }

    private synchronized boolean isDominated(int[] version) {

        Preconditions.checkArgument(versionVector.length == version.length, "Version vectors must have the same length to be compared.");

        for(int i = 0; i < versionVector.length; i++) {

            if(versionVector[i] > version[i]) return true;

        }

        return false;

    }

    private synchronized int[] increaseVersionVector() {

        versionVector[nodeId]++;

        return versionVector;

    }

}


