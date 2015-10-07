package de.tuberlin.pserver.runtime.filesystem.record.config;

import de.tuberlin.pserver.runtime.filesystem.record.IRecordIterator;

import java.io.InputStream;


public interface IRecordIteratorProducer {

    IRecordIterator getRecordIterator(InputStream inputStream);

}
