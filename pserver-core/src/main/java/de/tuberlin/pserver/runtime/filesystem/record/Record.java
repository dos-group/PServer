package de.tuberlin.pserver.runtime.filesystem.record;

import java.util.Iterator;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public interface Record<E, R> extends Iterator<E> {

    int size();

    E get(int i);

    E get(int i, R reusable);

    E next(R reusable);

}
