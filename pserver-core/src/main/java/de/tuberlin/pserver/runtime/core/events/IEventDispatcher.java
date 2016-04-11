package de.tuberlin.pserver.runtime.core.events;

import de.tuberlin.pserver.runtime.core.lifecycle.Deactivatable;

import java.util.List;

public interface IEventDispatcher extends Deactivatable {

    int getNumOfQueuedEvents();

    int getNumOfCachedEvents(String type);

    void addEventListener(String type, IEventHandler listener);

    void addEventListener(final String[] types, final IEventHandler listener);

    List<IEventHandler> getEventListener(final String type);

    boolean removeEventListener(String type, IEventHandler listener);

    boolean removeEventListener(String type);

    void removeAllEventListener();

    void dispatchEvent(Event event);

    boolean hasEventListener(String type);

    void joinDispatcherThread();

    Thread getDispatcherThread();
}
