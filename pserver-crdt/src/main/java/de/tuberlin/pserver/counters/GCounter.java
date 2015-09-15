package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;

public class GCounter extends AbstractCounter implements CRDT, Serializable {
    private int id;
    private int count = 0;

    public GCounter(int id, DataManager dataManager) {
        super(dataManager);
        this.id = id;

        dataManager.addDataEventListener("Operation", new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                applyUpdate(srcNodeID, (Operation) value, dataManager);
            }
        });
        
        run(dataManager);
    }

    public void add(int value, DataManager dm) {
        if(isAllNodesRunning()) {
            count += value;
            broadcast(ADD, value, dm);
        } else {
            buffer(value);
        }
    }

    public int getCount() {
        // Defensive copy so the value can not be changed without evoking the add method which broadcasts updates
        // TODO: Does this defensive copy make sense?
        int tmp = count;
        return tmp;
    }

    @Override
    public void applyUpdate(int srcNodeID, Operation op, DataManager dm){
        // receive from another node
        if(op.getType() == ADD) {
            count += op.getValue();
        } else if(op.getType() == END) {
            addFinishedNode(srcNodeID, dm);
        } else {
            // TODO: throw a specific exception
        }
    }

    public void broadcast(int op, int value, DataManager dm) {
        // send to all nodes
        dm.pushTo("Operation", new Operation(ADD, value), dm.remoteNodeIDs);
    }


}
