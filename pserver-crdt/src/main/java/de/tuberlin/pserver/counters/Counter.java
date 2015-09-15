package de.tuberlin.pserver.counters;

import de.tuberlin.pserver.crdt.CRDT;
import de.tuberlin.pserver.crdt.Operation;
import de.tuberlin.pserver.runtime.DataManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Counter implements CRDT, Serializable {
    private int id;
    private int count;
    private boolean allRunning = false;
    private Set runningNodes = new HashSet<Integer>();
    private Set finishedNodes = new HashSet<Integer>();
    private Set readyNodes = new HashSet<Integer>();
    //private DataManager dataManager;

    public Counter(int id, DataManager dataManager) {
        this.id = id;
        this.count = 0;
        //this.dataManager = dataManager;

        dataManager.addDataEventListener("Ready", new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                readyNodes.add(srcNodeID);
            }
        });

        dataManager.addDataEventListener("Running", new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                runningNodes.add(srcNodeID);
                if(runningNodes.size() == dataManager.remoteNodeIDs.length) {
                    allRunning = true;
                }
            }
        });

        dataManager.addDataEventListener("Operation", new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                applyUpdate(srcNodeID, (Operation) value, dataManager);
            }
        });

        dataManager.addDataEventListener("Finished", new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                finishedNodes.add(srcNodeID);
            }
        });

    }

    public void add(int value) {
        count += value;
        //if all are running => broadcast(ADD, value);
    }

    public void subtract(int value) {
        count -= value;
        //broadcast(SUBTRACT, value);
    }

    public int getCount() {
        // Defensive copy so the value can not be changed without evoking the add method which broadcasts updates
        int tmp = count;
        return tmp;
    }

    public Set getFinishedNodes() {
        return finishedNodes;
    }

    public Set getReadyNodes() {
        return readyNodes;
    }

    @Override
    public void applyUpdate(int srcNodeID, Operation op, DataManager dm){
        // receive from another node
        if(op.getType() == ADD) {
            add(op.getValue());
        } else if (op.getType() == SUBTRACT) {
            subtract(op.getValue());
        } else {
            // TODO: throw a specific exception
        }
    }

    public void broadcast(int op, int value, DataManager dm) {
        // send to all nodes
        //dataManager.pushTo("Operation", new Operation(op, value), dataManager.remoteNodeIDs);
    }

    public static void main(String[] args) {



    }
}
