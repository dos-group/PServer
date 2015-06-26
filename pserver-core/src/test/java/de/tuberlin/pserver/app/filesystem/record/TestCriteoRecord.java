package de.tuberlin.pserver.app.filesystem.record;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.local.LocalCriteoInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.ujmp.core.util.VerifyUtil.assertTrue;

public class TestCriteoRecord {

    private static String[] testString = {
            "1    5    110        16        1    0    14    7    1        306      62770d79    e21f5d58    afea442f    945c7fcf    38b02748  6fcd6dcb    3580aa21    28808903    46dedfa6    2e027dc1  0c7c4231    95981d1f    00c5ffb7    be4ee537    8a0b74cc  4cdc3efa    d20856aa    b8170bba    9512c20b    c38e2f28  14f65a5d    25b1b089    d7c1fc0b    7caf609c    30436bfc  ed10571d",
            "0    32    3    5        1    0    0    61    5    0    1    3157  5    e5f3fd8d    a0aaffa6    6faa15d5    da8a3421    3cd69f23  6fcd6dcb    ab16ed81    43426c29    1df5e154    7de9c0a9  6652dc64    99eb4e27    00c5ffb7    be4ee537    f3bbfe99  4cdc3efa    d20856aa    a1eb1511    9512c20b    febfd863  a3323ca1    c8e1ee56    1752e9e8    75350c8a    991321ea  b757e957",
            "0        233    1    146    1    0    0    99    7    0    1  3101    1    62770d79    ad984203    62bec60d    386c49ee  e755064d    6fcd6dcb    b5f5eb62    d1f2cc8b    2e4e821f  2e027dc1    0c7c4231    12716184    00c5ffb7    be4ee537  f70f0d0b    4cdc3efa    d20856aa    628f1b8d    9512c20b  c38e2f28    14f65a5d    25b1b089    d7c1fc0b    34a9b905  ff654802    ed10571d"
    };


    @Before
    public void setUp() throws Exception {
        BufferedWriter writer = null;
        try {
            File file = new File("/tmp/criteo");
            if (!file.exists()) {
                file.createNewFile();
                writer = new BufferedWriter(new FileWriter(file));
                for (String line : testString) {
                    writer.write(line);
                    writer.newLine();
                }
            }
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        finally {
            if(writer != null) {
                writer.close();
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        File file = new File("/tmp/criteo");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testParsing() {
        double[] checkArr = new double[] {1., 110., 16., 1., 0., 14., 7., 1., 306.};
        CriteoRecord rec = CriteoRecord.parse(testString[0], "[ \t]+", new int[]{0, 2, 3, 4, 5, 6, 7, 8, 9});
        assert(checkArr.length == rec.size());
        for(int i = 0; i < checkArr.length; i++) {
            assert(rec.get(i) == checkArr[i]);
        }

        String exception = "";
        try {
            CriteoRecord.parse(testString[0], "[ \t]+", new int[]{-1});
        }
        catch(RuntimeException e) {
            exception = e.getMessage();
        }
        assert(exception.equals("Index too small"));

        exception = "";
        try {
            CriteoRecord.parse(testString[0], "[ \t]+", new int[]{37});
        }
        catch(RuntimeException e) {
            exception = e.getMessage();
        }
        assert(exception.equals("Index too large"));

        try {
            CriteoRecord.parse(testString[0], "this delimiter does not exist", new int[]{0});
        }
        catch(RuntimeException e) {}

    }

    @Test
    public void testLocalCriteoInputFile() {
        double[][] checkArr = new double[][] {
                {1.,   5., 110.,  16., 1., 0., 14.,  7., 1., 306.},
                {0.,  32.,   3.,   5., 1., 0.,  0., 61., 5.,   0.},
                {0., 233.,   1., 146., 1., 0.,  0., 99., 7.,   0.}
        };
        LocalCriteoInputFile file = new LocalCriteoInputFile("/tmp/criteo", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        file.computeLocalFileSection(1, 0);
        FileDataIterator<CriteoRecord> iterator = file.iterator();
        CriteoRecord record = null;
        int line = 0;
        while(iterator.hasNext()) {
            record = iterator.next();
            for(int i = 0; i < checkArr.length; i++) {
                assertTrue(record.get(i) == checkArr[line][i], "row: %d, col: %d, exptected: %3$,.2f, actual: %4$,.2f", line, i, checkArr[line][i], record.get(i));
            }
            line++;
        }

    }

}
