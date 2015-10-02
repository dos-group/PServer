package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.AbstractOperation;
import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.Calendar;
import java.util.Date;

// TODO: in the Shapiro paper, concurrent updates are solved by concatenating MAC address to the timestamp...
// TODO: maybe don't even bother with the Date class, just do everything in long
// Last Writer Wins Register
public class LWWRegister<T extends Comparable> extends AbstractRegister<T> implements RegisterCRDT<T> {
    // TODO: this should be set to beginning of time or so
    private T register;
    private long time = Calendar.getInstance().getTimeInMillis();
    private final ConcurrentResolver<T> resolver;

    public LWWRegister(String id, DataManager dataManager, ConcurrentResolver<T> resolver) {
        super(id, dataManager);
        this.resolver = resolver;
        run(dataManager);
    }

    @Override
    protected boolean update(int srcNodeId, Operation<T> op, DataManager dm) {
        // TODO: is there a way to avoid this cast? It is on a critical path
        AbstractOperation.RegisterOperation<T> rop = (AbstractOperation.RegisterOperation<T>) op;

        if(rop.getType() == CRDT.WRITE) {
            return setRegister(rop.getValue(), rop.getDate().getTime());
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
    public boolean set(T element, DataManager dataManager) {
        if(setRegister(element, Calendar.getInstance().getTimeInMillis())) {
            broadcast(new AbstractOperation.RegisterOperation<>(CRDT.WRITE, element, new Date(time)), dataManager);
            return true;
        }
        return false;
    }
}