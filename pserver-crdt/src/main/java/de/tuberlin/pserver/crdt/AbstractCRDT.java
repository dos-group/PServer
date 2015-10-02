package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.crdt.operations.EndOperation;
import de.tuberlin.pserver.crdt.operations.IOperation;
import de.tuberlin.pserver.runtime.DataManager;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;


// TODO: what about exceptions in general
// TODO: what about when counters reach MAX_INT => exception or keep counting somehow?
// TODO: what if someone uses the same id for two crdts?
// TODO: what if only one node is running?
// TODO: maybe use blocking queues for buffers

public abstract class AbstractCRDT<T> implements CRDT<T> {
    private final Set<Integer> runningNodes;
    private final Set<Integer> finishedNodes;
    private final Queue<IOperation> buffer;

    protected final DataManager dataManager;
    protected final String id;

    private boolean allNodesRunning;
    private boolean allNodesFinished;


    public AbstractCRDT(String id, DataManager dataManager) {
        this.runningNodes = new HashSet<>();
        this.finishedNodes = new HashSet<>();
        this.buffer = new LinkedList<>();

        this.dataManager = dataManager;
        this.id = id;

        this.allNodesRunning = false;
        this.allNodesFinished = false;


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

        dataManager.addDataEventListener("Operation_" + id, new DataManager.DataEventHandler() {
            @Override
            public void handleDataEvent(int srcNodeID, Object value) {
                if (((IOperation) value).getType() == CRDT.END) {
                    addFinishedNode(srcNodeID, dataManager);
                    //inBuffer.add(new RegisterOperation<Integer>(END, null, null));
                    //update(srcNodeID, (IOperation) value, dataManager);
                } else {
                    //inBuffer.add((IOperation)value);
                    update(srcNodeID, (IOperation) value);
                }
            }
        });
    }

    @Override
    public void run(DataManager dataManager) {
        dataManager.pushTo("Running_" + id, 0, dataManager.remoteNodeIDs);
    }

    @Override
    public void finish(DataManager dataManager) {
        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());

        while(!isAllNodesRunning()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("[DEBUG] All nodes: " + isAllNodesRunning());
        System.out.println("[DEBUG] BufferB: " + buffer.size());

        broadcast(new EndOperation(), dataManager);

        while(!isAllNodesFinished()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isAllNodesRunning() {
        return allNodesRunning;
    }

    private boolean isAllNodesFinished() {
        return allNodesFinished;
    }

    public Queue getBuffer() {
        return this.buffer;
    }
    //public Queue getInBuffer() { return this.inBuffer; }

    protected void addFinishedNode(int nodeID, DataManager dataManager) {
        finishedNodes.add(nodeID);
        //allNodesRunning = false;
        if(finishedNodes.size() == dataManager.remoteNodeIDs.length) {
            allNodesFinished = true;
        }
    }

    protected void broadcast(IOperation op, DataManager dm) {
        // send to all nodes
        if(isAllNodesRunning()) {
            broadcastBuffer(dm);
            dm.pushTo("Operation_"+id, op, dm.remoteNodeIDs);
        } else {
            buffer(op);
        }
    }

    private void buffer(IOperation op) {
        buffer.add(op);
    }

    protected abstract boolean update(int srcNodeId, IOperation<T> op);

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
}
