package de.tuberlin.pserver.radt;

import java.io.Serializable;
import java.util.stream.IntStream;

public class S4Vector implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int sessionNumber;

    private final int siteId;

    private final int vectorClockSum;

    private final int seq;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // no args constructor for serialization
    public S4Vector() {

        this.sessionNumber = 0;

        this.siteId = 0;

        this.vectorClockSum = 0;

        this.seq = 0;

    }

    public S4Vector(int siteId, int[] vectorClock) {
        /*
         * From Roh et al. (2010):
         * "As a unit of collaboration, a session begins with initial vector clocks and identical RADT structures at all
         *  sites. When a membership changes or a collaboration newly begins with the same RADT structure stored on disk,
         *  s_o[ssn] increases."
         *
         *  As we are not allowing membership of an RADT to change or members to be added later, the sessionNumber will
         *  never increase in our implementation.
         */

        this.sessionNumber = 0;

        this.siteId = siteId;

        this.vectorClockSum = IntStream.of(vectorClock).sum();

        this.seq = vectorClock[siteId];

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

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

    public boolean precedes(S4Vector other) {

        return  (this.sessionNumber < other.getSessionNumber()) ||

                (this.sessionNumber == other.sessionNumber
                        && this.vectorClockSum < other.getVectorClockSum()) ||

                (this.sessionNumber == other.getSessionNumber()
                        && this.vectorClockSum == other.getVectorClockSum()
                        && this.siteId < other.siteId);

    }

    @Override
    public String toString() {

        return "<" + sessionNumber + ", " + siteId + ", " + vectorClockSum + ", " + seq + ">";

    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        S4Vector s4Vector = (S4Vector) o;

        if (sessionNumber != s4Vector.sessionNumber) return false;

        if (siteId != s4Vector.siteId) return false;

        if (vectorClockSum != s4Vector.vectorClockSum) return false;

        return seq == s4Vector.seq;

    }

    @Override
    public int hashCode() {

        int result = sessionNumber;

        result = 31 * result + siteId;

        result = 31 * result + vectorClockSum;

        result = 31 * result + seq;

        return result;

    }

}
