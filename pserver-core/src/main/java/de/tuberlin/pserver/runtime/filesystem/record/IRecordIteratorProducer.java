package de.tuberlin.pserver.runtime.filesystem.record;

import java.io.InputStream;


public interface IRecordIteratorProducer {

    IRecordIterator getRecordIterator(InputStream inputStream);

}
