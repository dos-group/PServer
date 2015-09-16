package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.*;


// TODO: improve buffer performance => sometimes one or two operations are missing at a replica
// TODO: what about exceptions in general
// TODO what about when counters reach MAX_INT => exception or keep counting somehow?

public abstract class AbstractCRDT implements CRDT {
    private final Set runningNodes = new HashSet<>();
    private final Set finishedNodes = new HashSet<>();
    private final Queue<Operation> buffer = new LinkedList();

    private boolean allNodesRunning = false;
    private boolean allNodesFinished = false;
    protected final String id;


    public AbstractCRDT(String id, DataManager dataManager) {

        this.id = id;

        dataManager.addDataEventListener("Running_"+id, new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                runningNodes.add(srcNodeID);
                System.out.println("[DEBUG] Running: " + runningNodes.size());

                if (runningNodes.size() == dataManager.remoteNodeIDs.length) {
                    allNodesRunning = true;
                    dataManager.removeDataEventListener("Running_"+id, this);

                    // This is necessary to reach replicas that were not online when the first "Running" message was sent
                    dataManager.pushTo("Running_"+id, 0, dataManager.remoteNodeIDs);
                    System.out.println("[DEBUG] BufferA: " + buffer.size());
                    //broadcastBuffer(dataManager);
                }
            }
        });

        dataManager.addDataEventListener("Operation_"+id, new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                update(srcNodeID, (Operation) value, dataManager);
            }
        });
    }


    public void run(DataManager dataManager) {
        dataManager.pushTo("Running_" + id, 0, dataManager.remoteNodeIDs);
    }

    public void finish(DataManager dataManager) {
        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());

        while(!isAllNodesRunning()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

       // broadcastBuffer(dataManager);

        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());
        System.out.println("[DEBUG] BufferB: " + buffer.size());

        broadcast(new Operation(END, 0), dataManager);

        while(!isAllNodesFinished()) {
            try {
                //broadcastBuffer(dataManager);
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
        //allNodesRunning = false;
        if(finishedNodes.size() == dataManager.remoteNodeIDs.length) {
            allNodesFinished = true;
        }
    }

    protected void broadcast(Operation op, DataManager dm) {
        // send to all nodes
        if(isAllNodesRunning()) {
            broadcastBuffer(dm);
            dm.pushTo("Operation_"+id, op, dm.remoteNodeIDs);
        } else {
            buffer(op);
        }
    }

    private void broadcastBuffer(DataManager dm) {
        if(buffer.size() > 0) {
            System.out.println("[DEBUG] Broadcasting buffer size " + buffer.size());
            while(buffer.size() > 0) {
                dm.pushTo("Operation_"+id, buffer.poll(), dm.remoteNodeIDs);
            }
        } else {
            //TODO: some exception?
        }
    }

    protected void buffer(Operation op) {
        buffer.add(op);
    }

    public void applyOperation(Operation op, DataManager dm) {
        update(-1, op, dm);
        broadcast(op, dm);
    }

    public Queue getBuffer() {
        return buffer;
    }

    protected abstract void update(int srcNodeId, Operation op, DataManager dm);

}
