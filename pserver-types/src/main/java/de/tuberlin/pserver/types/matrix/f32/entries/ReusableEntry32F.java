package de.tuberlin.pserver.types.matrix.f32.entries;


public interface ReusableEntry32F extends Entry32F {

    ReusableEntry32F set(long row, long col, float value);
}
