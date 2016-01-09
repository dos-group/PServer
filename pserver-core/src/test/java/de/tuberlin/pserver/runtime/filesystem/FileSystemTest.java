package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.filesystem.records.RecordIterator;
import de.tuberlin.pserver.runtime.state.entries.Entry;
import de.tuberlin.pserver.runtime.state.entries.ImmutableEntryImpl;
import de.tuberlin.pserver.runtime.state.entries.MutableEntryImpl;
import de.tuberlin.pserver.runtime.state.entries.ReusableEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Created by hegemon on 23.12.15.
 */
public class FileSystemTest {

    private InputStream inputStream;

    @Before
    public void setup() {
        try {
            this.inputStream = new FileInputStream("../datasets/heart_scale.libsvm");
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            this.inputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEntries() {
        Entry<Float> entry = new ImmutableEntryImpl<>(1, 1, 0.1F);
        assertEquals(entry.getValue(), new Float(0.1));

        ReusableEntry<Double> reusableEntry = new MutableEntryImpl<>(2, 3, 0.3);
        reusableEntry.set(2, 3, 0.4);
        assertEquals(reusableEntry.getValue(), new Double(0.4));
    }

    @Test
    public void testRecordWithoutProjection() {

        RecordIterator iterator =
            RecordIterator.create(FileFormat.SVM_FORMAT, inputStream, Optional.empty());

        Record record;
        while (iterator.hasNext()) {
            record = iterator.next();
            System.out.print(record.getTarget() + " | Row " + record.getRow()  + " ( ");
            while (record.hasNext()) {
                Entry entry = record.next();
                System.out.print(entry.toString() + " ");
            }
            System.out.println(" )");
        }
    }

    @Test
    public void testRecordWithProjection() {

        RecordIterator iterator =
                RecordIterator.create(FileFormat.SVM_FORMAT, inputStream, Optional.of(new long[]{ 1L, 2L, 7L }));

        Record record;
        while (iterator.hasNext()) {
            record = iterator.next();
            System.out.print(record.getTarget() + " | Row " + record.getRow() + " ( ");
            while (record.hasNext()) {
                Entry entry = record.next();
                System.out.print(entry.toString() + " ");
            }
            System.out.println(" )");
        }
    }

}
