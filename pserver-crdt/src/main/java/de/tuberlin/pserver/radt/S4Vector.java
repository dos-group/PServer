package de.tuberlin.pserver.radt;

import java.io.Serializable;

public class S4Vector implements Serializable {
    private final int sessionNumber;
    private final int siteId;
    private final int vectorClockSum;
    // TODO: find a better name for this but right now I don't really know what it does...
    // this is somehow for purging tombstones
    private final int seq;

    public S4Vector(int sessionNumber, int siteId, int[] vectorClock, int seq) {
        this.sessionNumber = sessionNumber;
        this.siteId = siteId;

        int sum = 0;
        for(int i : vectorClock) {
            sum = sum + i;
        }
        this.vectorClockSum = sum;

        this.seq = seq;
    }

    public int getSessionNumber() {
        return sessionNumber;
    }

    public int getSiteId() {
        return siteId;
    }

    public int getVectorClockSum() {
        return vectorClockSum;
    }

    public int getSeq() {
        return seq;
    }

    public boolean takesPrecedenceOver(S4Vector other) {
        return (this.sessionNumber < other.getSessionNumber()) ||
                (this.sessionNumber == other.sessionNumber
                        && this.vectorClockSum < other.getVectorClockSum()) ||
                (this.sessionNumber == other.getSessionNumber()
                        && this.vectorClockSum == other.getVectorClockSum()
                        && this.siteId < other.siteId);
    }
}
