package de.tuberlin.pserver.runtime.core.events;

import de.tuberlin.pserver.runtime.core.common.Deactivatable;

import java.util.List;

public interface IEventDispatcher extends Deactivatable {

    public abstract int getNumOfQueuedEvents();

    public abstract int getNumOfCachedEvents(String type);

    public abstract void addEventListener(String type, IEventHandler listener);

    public abstract void addEventListener(final String[] types, final IEventHandler listener);

    public abstract List<IEventHandler> getEventListener(final String type);

    public abstract boolean removeEventListener(String type, IEventHandler listener);

    public abstract void removeAllEventListener();

    public abstract void dispatchEvent(Event event);

    public abstract boolean hasEventListener(String type);

    public abstract void joinDispatcherThread();

    public abstract Thread getDispatcherThread();
}
