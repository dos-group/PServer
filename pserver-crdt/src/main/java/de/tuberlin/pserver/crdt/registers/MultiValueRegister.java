package de.tuberlin.pserver.crdt.registers;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.exceptions.IllegalOperationException;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.crdt.operations.TaggedOperation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

// Not exactly sure how this one works, apparently non-concurrent updates have the usual register semantics but
    // concurrent assignments create a set of values in the register

    public class MultiValueRegister<T> extends AbstractRegister<T> implements Register<T> {
        // TODO: this should be set to beginning of time or so
        private long time = Calendar.getInstance().getTimeInMillis();
        private Set<T> register = new HashSet<T>();

        public MultiValueRegister(String id, DataManager dataManager) {
            super(id, dataManager);
            run(dataManager);
        }

        @Override
        protected boolean update(int srcNodeId, IOperation<T> op) {
            // TODO: is there a way to avoid this cast? It is on a critical path
            TaggedOperation<T,Date> rop = (TaggedOperation<T,Date>) op;

            if(rop.getType() == CRDT.WRITE) {
                if(rop.getTag().getTime() > this.time) {
                    return setRegister(rop.getValue(), rop.getTag().getTime());
                }
                else if(rop.getTag().getTime() == this.time) {
                    return appendToRegister(rop.getValue());
                }

                return false;
            }
            else {
                // TODO: create a good error message
                throw new IllegalOperationException("Blub");
            }
        }

        @Override
        public boolean set(T element) {
            long t = Calendar.getInstance().getTimeInMillis();
            if(t > this.time) {
                setRegister(element, t);
                broadcast(new TaggedOperation<>(CRDT.WRITE, element, new Date(t)), dataManager);
                return true;
            }
            else if(t == this.time) {
                appendToRegister(element);
                broadcast(new TaggedOperation<>(CRDT.WRITE, element, new Date(t)), dataManager);
                return true;
            }

            return false;
        }

        public Set<T> getRegister() {
            return this.register;
        }

        private boolean setRegister(T element, long time) {
            this.time = time;
            this.register.clear();
            this.register.add(element);
            return true;
        }

        private boolean appendToRegister(T element) {
            this.register.add(element);
            return true;
        }


    }


