package de.tuberlin.pserver.registers;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.Calendar;
import java.util.Date;

// TODO: allow passing in a custom resolveConcurrent method
// Last Writer Wins Register
public class LWWRegister<T extends Comparable> extends AbstractRegister<T> implements RegisterCRDT<T> {
    // TODO: this should be set to beginning of time or so
    private Date date = Calendar.getInstance().getTime();
    private ConcurrentResolver<T> resolver;

    public LWWRegister(String id, DataManager dataManager, ConcurrentResolver<T> resolver) {
        super(id, dataManager);
        this.resolver = resolver;
        run(dataManager);
    }

    @Override
    protected void update(int srcNodeId, Operation op, DataManager dm) {
        // TODO: is there a way to avoid this cast? It is on a critical path
        RegisterOperation<T> rop = (RegisterOperation) op;

        //System.out.println(rop.getValue() + "; " + rop.getDate().getTime());
        if (rop.getDate().after(this.date)) {
            updateRegister(rop.getValue(), rop.getDate());
        }
        else if(rop.getDate().equals(this.date)) {
            if(resolver.resolveConcurrent(rop.getValue(), getValue())) {
                updateRegister(rop.getValue(), rop.getDate());
            }
        }
    }

    private void updateRegister(T value, Date date) {
        this.date = date;
        setValue(value);
    }
}