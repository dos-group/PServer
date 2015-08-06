package de.tuberlin.pserver.runtime.memory;

import de.tuberlin.pserver.commons.ds.IntervalTree;

final class LockToken {

    public final IntervalTree.Interval in;

    public LockToken(final int s, final int e) { this.in = new IntervalTree.Interval(s, e); }
}
