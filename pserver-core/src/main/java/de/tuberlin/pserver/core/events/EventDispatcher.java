package de.tuberlin.pserver.core.events;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventDispatcher implements IEventDispatcher {

    // ---------------------------------------------------

    public final class InternalEventTypes {

        private InternalEventTypes() {}

        public static final String KILL_EVENT = "KILL_EVENT";
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(EventDispatcher.class);

    private final Map<String, List<IEventHandler>> listenerMap;

    private final boolean useDispatchThread;

    private final Thread dispatcherThread;

    private final AtomicBoolean isRunning;

    private final BlockingQueue<Event> eventQueue;

    private final Map<String,List<Event>> cachedEvents;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public EventDispatcher(boolean useDispatchThread) {
        this(useDispatchThread, null);
    }
    public EventDispatcher(boolean useDispatchThread, String name) {

        this.listenerMap        = new ConcurrentHashMap<>();
        this.cachedEvents       = new ConcurrentHashMap<>();
        this.useDispatchThread  = useDispatchThread;
        this.isRunning          = new AtomicBoolean(useDispatchThread);
        this.eventQueue         = useDispatchThread ? new LinkedBlockingQueue<>() : null;

        if (useDispatchThread) {
            final Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    while (isRunning.get() || eventQueue.size() != 0) {
                        try {
                            final Event event = eventQueue.take();
                            LOG.trace("Process event {} - events left in queue: {}", event.type, eventQueue.size());
                            // Stop dispatching thread, if a poison pill was received.
                            if (!event.type.equals(InternalEventTypes.KILL_EVENT)) {
                                if (!dispatch(event)) {
                                    if (event.isSticky) {
                                        List<Event> events = cachedEvents.get(event.type);
                                        if (events == null) {
                                            events = new ArrayList<>();
                                            cachedEvents.put(event.type, events);
                                        }
                                        events.add(event);
                                    }
                                }
                            } else {
                                LOG.trace("kill dispatcher thread");
                            }
                        } catch (InterruptedException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                        }
                    }
                    removeAllEventListener();
                }
            };
            if (name != null) {
                this.dispatcherThread = new Thread(runnable, name);
            } else {
                this.dispatcherThread = new Thread(runnable);
            }
            this.dispatcherThread.start();
        } else {
            this.dispatcherThread = null;
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int getNumOfQueuedEvents() {
        return eventQueue.size();
    }

    @Override
    public int getNumOfCachedEvents(final String type) {
        final List<Event> events = cachedEvents.get(type);
        return events == null ? 0 : events.size();
    }

    @Override
    public synchronized void addEventListener(final String type, final IEventHandler listener) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(listener);
        List<IEventHandler> listeners = listenerMap.get(type);
        if (listeners == null) {
            listeners = Collections.synchronizedList(new LinkedList<>());
            listenerMap.put(type, listeners);
        }
        listeners.add(listener);

        if (cachedEvents.containsKey(type)) {
            final List<Event> events = cachedEvents.remove(type);
            events.forEach(this::dispatch);
        }
    }

    @Override
    public synchronized void addEventListener(final String[] types, final IEventHandler listener) {
        Preconditions.checkNotNull(types);
        Preconditions.checkNotNull(listener);
        for (final String type : types) {
            addEventListener(type, listener);
        }
    }

    @Override
    public List<IEventHandler> getEventListener(final String type) {
        Preconditions.checkNotNull(type);
        return listenerMap.get(type);
    }

    @Override
    public synchronized boolean removeEventListener(final String type, final IEventHandler listener) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(listener);
        final List<IEventHandler> listeners = listenerMap.get(type);
        if (listeners != null) {
            boolean isRemoved = listeners.remove(listener);
            // if no more listeners registered, we remove the complete mapping.
            if (isRemoved && listeners.size() == 0) {
                listenerMap.remove(type);
            }
            return isRemoved;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void removeAllEventListener() {
        listenerMap.values().forEach(List<IEventHandler>::clear);
        listenerMap.clear();
    }

    @Override
    public synchronized void dispatchEvent(final Event event) {
        Preconditions.checkNotNull(event);
        if (useDispatchThread) {
            eventQueue.offer(event);
        } else {
            if (!dispatch(event)) {
                if (event.isSticky) {
                    List<Event> events = cachedEvents.get(event.type);
                    if (events == null) {
                        events = new ArrayList<>();
                        cachedEvents.put(event.type, events);
                    }
                    events.add(event);
                }
            }
        }
    }

    @Override
    public boolean hasEventListener(final String type) {
        Preconditions.checkNotNull(type);
        return listenerMap.get(type) != null;
    }

    public void setName(String name) {
        Preconditions.checkNotNull(name);
        if (useDispatchThread) {
            this.dispatcherThread.setName(name);
        }
    }

    @Override
    public void deactivate() {
        if (useDispatchThread) {
            isRunning.set(false);
            // Feed the poison pill to the event dispatcher thread to terminate it.
            eventQueue.add(new Event(InternalEventTypes.KILL_EVENT));
        } else {
            removeAllEventListener();
        }
    }

    @Override
    public void joinDispatcherThread() {
        if (useDispatchThread) {
            try {
                dispatcherThread.join();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
            }
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private boolean dispatch(final Event event) {
        Preconditions.checkNotNull(event);

        List<IEventHandler> listeners = null;
        int retryCount = 5;
        while (listeners == null && retryCount-- > 0) { // TODO: ??
            listeners = listenerMap.get(event.type);
            if (listeners == null) {
                try {
                    Thread.sleep(5);
                } catch(InterruptedException ie) {
                    LOG.error(ie.getMessage());
                }
            }
        }

        if (listeners != null) {
            List<IEventHandler> l = new ArrayList<>(listeners);
            for (final IEventHandler el : l) {
                try {
                    el.handleEvent(event);
                } catch (Throwable t) {
                    LOG.error(t.getLocalizedMessage(), t);
                    throw t;
                }
            }
        } else { // listeners == null
            LOG.debug("no listener registered for event " + event.type);
            // Event wasn't processed by any event handler.
            return false;
        }
        return true;
    }
}