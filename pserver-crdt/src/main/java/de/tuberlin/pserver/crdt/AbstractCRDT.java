package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.registers.RegisterOperation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;


// TODO: improve buffer performance => sometimes one or two operations are missing at a replica
// TODO: what about exceptions in general
// TODO: what about when counters reach MAX_INT => exception or keep counting somehow?
// TODO: what if someone uses the same id for two crdts?
// TODO: what if only one node is running?
// TODO: maybe use blocking queues for buffers

public abstract class AbstractCRDT implements CRDT {
    private final Set<Integer> runningNodes = new HashSet<>();
    private final Set<Integer> finishedNodes = new HashSet<>();
    private final Queue<Operation> outBuffer = new LinkedList<>();
    //private final Queue<Operation> inBuffer = new LinkedBlockingQueue<>();

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
                    System.out.println("[DEBUG] BufferA: " + outBuffer.size());
                    //broadcastBuffer(dataManager);
                }
            }
        });

        dataManager.addDataEventListener("Operation_"+id, new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                if(((Operation)value).getType() == CRDT.END) {
                    addFinishedNode(srcNodeID, dataManager);
                    //inBuffer.add(new RegisterOperation<Integer>(END, null, null));
                    //update(srcNodeID, (Operation) value, dataManager);
                } else {
                    //inBuffer.add((Operation)value);
                    update(srcNodeID, (Operation) value, dataManager);
                }
            }
        });
    }


    public void run(DataManager dataManager) {
        dataManager.pushTo("Running_" + id, 0, dataManager.remoteNodeIDs);
    }

    public void finish(DataManager dataManager) {
        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());

        while(!isAllNodesRunning()) {// && !(inBuffer.size() == 0) && !(outBuffer.size() == 0)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

       // broadcastBuffer(dataManager);

        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());
        System.out.println("[DEBUG] BufferB: " + outBuffer.size());

        broadcast(new Operation(END), dataManager);

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
        if(outBuffer.size() > 0) {
            System.out.println("[DEBUG] Broadcasting buffer size " + outBuffer.size());
            while(outBuffer.size() > 0) {
                dm.pushTo("Operation_"+id, outBuffer.poll(), dm.remoteNodeIDs);
            }
        } else {
            //TODO: some exception?
        }
    }

    protected void buffer(Operation op) {
        outBuffer.add(op);
    }

    public void applyOperation(Operation op, DataManager dm) {
        //inBuffer.add(op);
        update(-1, op, dm);
        broadcast(op, dm);
    }

    public Queue getBuffer() {
        return this.outBuffer;
    }
    //public Queue getInBuffer() { return this.inBuffer; }

    protected abstract void update(int srcNodeId, Operation op, DataManager dm);

}
