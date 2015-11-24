package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.Calendar;
import java.util.Date;

// TODO: in the Shapiro paper, concurrent updates are solved by concatenating MAC address to the timestamp...
// TODO: maybe don't even bother with the Date class, just do everything in long
// Last Writer Wins Register
public class LWWRegister<T extends Comparable> extends AbstractRegister<T> implements Register<T> {
    // TODO: this should be set to beginning of time or so
    private T register;
    private long time = Calendar.getInstance().getTimeInMillis();
    private final ConcurrentResolver<T> resolver;

    public LWWRegister(String id, int noOfReplicas, ProgramContext programContext, ConcurrentResolver<T> resolver) {
        super(id, noOfReplicas, programContext);
        this.resolver = resolver;
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        // TODO: is there a way to avoid this cast? It is on a critical path
        TaggedOperation<T,Date> rop = (TaggedOperation<T, Date>) op;

        if(rop.getType() == Operation.WRITE) {
            return setRegister(rop.getValue(), rop.getTag().getTime());
        }
        else {
            // TODO: error text
            throw new IllegalOperationException("Whaaaa");
        }
    }

    public T getRegister() {
        return this.register;
    }

    private boolean setRegister(T element, long time) {
        if(time > this.time) {
            this.register = element;
            this.time = time;
            return true;
        }
        else if(time == this.time) {
            if(resolver.resolveConcurrent(element, this.register)) {
                this.register = element;
                this.time = time;
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean set(T element) {
        if(setRegister(element, Calendar.getInstance().getTimeInMillis())) {
            broadcast(new TaggedOperation<>(Operation.WRITE, element, new Date(time)));
            return true;
        }
        return false;
    }
}