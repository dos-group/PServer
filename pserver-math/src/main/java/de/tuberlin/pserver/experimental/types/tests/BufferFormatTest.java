package de.tuberlin.pserver.experimental.types.tests;


import de.tuberlin.pserver.commons.UnsafeOp;
import de.tuberlin.pserver.experimental.memory.Types;

public class BufferFormatTest {

    private BufferFormatTest() {}

    public static long toLong(byte[] data) {
        if (data == null || data.length != 8) return 0x0;
        // ----------
        return (long)(
                (long)(0xff & data[7]) << 56  |
                (long)(0xff & data[6]) << 48  |
                (long)(0xff & data[5]) << 40  |
                (long)(0xff & data[4]) << 32  |
                (long)(0xff & data[3]) << 24  |
                (long)(0xff & data[2]) << 16  |
                (long)(0xff & data[1]) << 8   |
                (long)(0xff & data[0]) << 0
        );
    }

    public static double toDouble(byte[] data) {
        if (data == null || data.length != 8) return 0x0;
        return Double.longBitsToDouble(toLong(data));
    }

    public static double[] toDoubleArray(byte[] data) {
        if (data == null) return null;
        if (data.length % 8 != 0) return null;
        double[] dbls = new double[data.length / 8];
        for (int i = 0; i < dbls.length; i++) {
            dbls[i] = toDouble( new byte[] {
                    data[(i*8)],
                    data[(i*8)+1],
                    data[(i*8)+2],
                    data[(i*8)+3],
                    data[(i*8)+4],
                    data[(i*8)+5],
                    data[(i*8)+6],
                    data[(i*8)+7],
            } );
        }
        return dbls;
    }

    public static void main(final String[] args) {

        final byte[] buffer = new byte[8 * 256 * 256];

        UnsafeOp.unsafe.putDouble(buffer, (long)(UnsafeOp.BYTE_ARRAY_OFFSET + (3 * 256 * Types.DOUBLE_TYPE_INFO.size()) + (12 * Types.DOUBLE_TYPE_INFO.size())), 1.234);

        //double[] dBuffer = toDoubleArray(buffer);

        //System.out.println("==> " + dBuffer[3 * 256 + 12]);

    }
}
