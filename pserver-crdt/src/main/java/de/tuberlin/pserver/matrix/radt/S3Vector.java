package de.tuberlin.pserver.matrix.radt;

import java.io.Serializable;

public class S3Vector implements Serializable {
    private long vectorClockSum;
    private long sessionID;
    private final int siteID;

    public S3Vector(long[] vectorClock, long sessionID, int siteID) {
        this.sessionID = sessionID;
        this.siteID = siteID;

        // Initialize vector clock sum
        long sum = 0;
        for(int i = 0; i < vectorClock.length; i++) {
            sum = Math.addExact(sum, vectorClock[i]);
        }
        this.vectorClockSum = sum;
    }

    public void setVectorClockSum(long[] vectorClock) {
        // Initialize vector clock sum
        long sum = 0;
        for(int i = 0; i < vectorClock.length; i++) {
            sum = Math.addExact(sum, vectorClock[i]);
        }
        this.vectorClockSum = sum;
    }

    public void setSessionID(long sessionID) {
        this.sessionID = sessionID;
    }

    public long getVectorClockSum() {
        return vectorClockSum;
    }

    public long getSessionID() {
        return sessionID;
    }

    public long getSiteID() {
        return siteID;
    }

    public boolean takesPrecedenceOver(S3Vector other) {
        if(this.sessionID < other.sessionID) return true;
        else if(this.sessionID > other.sessionID) return false;

        if(this.vectorClockSum < other.vectorClockSum) return true;
        else if(this.vectorClockSum > other.vectorClockSum) return false;

        // In this case the operations are concurrent
        // TODO: allow user defined resolution of concurrent updates
        if(this.siteID < other.siteID) return true;

        return false;
    }
}
