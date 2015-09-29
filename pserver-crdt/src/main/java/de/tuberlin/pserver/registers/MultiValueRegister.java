package de.tuberlin.pserver.registers;

import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

// Not exactly sure how this one works, apparently non-concurrent updates have the usual register semantics but
    // concurrent assignments create a set of values in the register

    public class MultiValueRegister<T> extends AbstractRegister<Set<T>> implements RegisterCRDT<Set<T>> {
        // TODO: this should be set to beginning of time or so
        private Date date = Calendar.getInstance().getTime();
        private Set<T> newValue = new HashSet<T>();

        public MultiValueRegister(String id, DataManager dataManager) {
            super(id, dataManager);
            run(dataManager);
        }

        @Override
        protected boolean update(int srcNodeId, Operation<Set<T>> op, DataManager dm) {
            // TODO: is there a way to avoid this cast? It is on a critical path
            RegisterOperation<T> rop = (RegisterOperation<T>) op;

            if (rop.getDate().after(this.date)) {
                newValue.clear();
                newValue.add(rop.getValue());
                updateRegister(newValue, rop.getDate());
            }
            else if(rop.getDate().equals(this.date)) {
                appendToRegister(rop.getValue());
            }
            return true;
        }

        private void updateRegister(Set<T> value, Date date) {
            this.date = date;
            setValue(value);
        }

        private void appendToRegister(T value) {
            getValue().add(value);
        }
    }


