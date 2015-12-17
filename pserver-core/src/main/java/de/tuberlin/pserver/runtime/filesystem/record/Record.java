package de.tuberlin.pserver.runtime.filesystem.record;

import java.util.Iterator;

/**
 * Created by Morgan K. Geldenhuys on 15.12.15.
 */
public interface Record<T1, T2> extends Iterator<T1> {

    int size();

    T1 get(int i);

    T1 get(int i, T2 reusable);

    T1 next(int i, T2 reusable);

}
