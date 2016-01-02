package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.runtime.filesystem.record.MatrixRecord;
import de.tuberlin.pserver.runtime.filesystem.record.RecordIterator;
import de.tuberlin.pserver.runtime.state.mtxentries.MatrixEntry;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by hegemon on 23.12.15.
 */
public class FileSystemTest {

    private InputStream inputStream;

    @Before
    public void setup() {
        try {
            this.inputStream = new FileInputStream("../datasets/X_train.csv");
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
    public void testRecord() {

        RecordIterator iterator = RecordIterator.create(Format.DENSE_FORMAT, inputStream);

        MatrixRecord matrixRecord;
        int currentLine = 0;
        while (iterator.hasNext()) {
            matrixRecord = (MatrixRecord) iterator.next(currentLine);
            System.out.print("Row " + currentLine + " ");
            for (int i = 0; i < matrixRecord.size(); i++) {
                System.out.print(matrixRecord.get(i) + " ");
            }
            System.out.println();
            currentLine++;
        }
    }

}
