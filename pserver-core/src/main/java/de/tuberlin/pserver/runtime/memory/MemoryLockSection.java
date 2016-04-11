package de.tuberlin.pserver.runtime.memory;

import com.google.common.base.Preconditions;

import java.util.concurrent.locks.ReentrantLock;

final class MemoryLockSection {

    public final long threadID;

    public final LockToken token;

    private ReentrantLock mutex;

    // ---------------------------------------------------

    public MemoryLockSection(final long threadID, final int s, final int e) {
        this.threadID   = threadID;
        this.token      = new LockToken(s, e);
    }

    // ---------------------------------------------------

    public void lock() { Preconditions.checkNotNull(mutex).lock(); }

    public void unlock() { Preconditions.checkNotNull(mutex).unlock(); mutex = null; }

    public void createMutex() { mutex = new ReentrantLock(); }

    public void transferMutex(final MemoryLockSection section) {
        Preconditions.checkNotNull(section);
        Preconditions.checkState(this.mutex == null);
        this.mutex = Preconditions.checkNotNull(section.mutex);
    }
}
