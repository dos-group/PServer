package de.tuberlin.pserver.app.memmng;

import de.tuberlin.pserver.utils.IntervalTree;

final class LockToken {

    public final IntervalTree.Interval in;

    public LockToken(final int s, final int e) { this.in = new IntervalTree.Interval(s, e); }
}
