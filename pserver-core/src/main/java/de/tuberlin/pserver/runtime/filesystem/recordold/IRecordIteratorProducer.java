package de.tuberlin.pserver.runtime.filesystem.recordold;

import java.io.InputStream;


public interface IRecordIteratorProducer {

    IRecordIterator getRecordIterator(InputStream inputStream);

}
