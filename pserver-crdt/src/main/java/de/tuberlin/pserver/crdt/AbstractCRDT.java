package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractCRDT implements CRDT {
    private Set runningNodes = new HashSet<>();
    private Set finishedNodes = new HashSet<>();
    private boolean allNodesRunning = false;
    private boolean allNodesFinished = false;

    public AbstractCRDT(DataManager dataManager) {

        dataManager.addDataEventListener("Running", new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                runningNodes.add(srcNodeID);
                System.out.println("[DEBUG] Running: " + runningNodes.size());

                if (runningNodes.size() == dataManager.remoteNodeIDs.length) {
                    allNodesRunning = true;
                    dataManager.removeDataEventListener("Running", this);

                    // This is necessary to reach replicas that were not online when the first "Running" message was sent
                    dataManager.pushTo("Running", 0, dataManager.remoteNodeIDs);
                    System.out.println("[DEBUG] BufferA: " + getBuffer());
                    add(getBuffer(), dataManager);
                    resetBuffer();
                }
            }
        });
    }


    public void run(DataManager dataManager) {
        dataManager.pushTo("Running", 0, dataManager.remoteNodeIDs);
    }

    public void finish(DataManager dataManager) {
        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());

        while(!isAllNodesRunning()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());

        System.out.println("[DEBUG] BufferB: " + getBuffer());
        add(getBuffer(), dataManager);
        resetBuffer();

        dataManager.pushTo("Operation", new Operation(END, 0), dataManager.remoteNodeIDs);

        while(!isAllNodesFinished()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isAllNodesRunning() {
        return allNodesRunning;
    }

    public boolean isAllNodesFinished() {
        return allNodesFinished;
    }

    protected void addFinishedNode(int nodeID, DataManager dataManager) {
        finishedNodes.add(nodeID);
        if(finishedNodes.size() == dataManager.remoteNodeIDs.length) {
            allNodesFinished = true;
        }
    }

    protected abstract void buffer(int value);
    protected abstract int getBuffer();
    protected abstract void broadcast(int op, int value, DataManager dm);
    protected abstract void resetBuffer();
    protected abstract void add(int value, DataManager dm);
}
