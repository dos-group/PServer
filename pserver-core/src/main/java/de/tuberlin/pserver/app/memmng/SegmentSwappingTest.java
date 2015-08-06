package de.tuberlin.pserver.app.memmng;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.compression.Compressor;
import de.tuberlin.pserver.core.common.Deactivatable;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SegmentSwappingTest {

    public static final class TmpFileWriter implements Runnable, Deactivatable {

        // ---------------------------------------------------

        private volatile boolean isRunning = true;

        private final BlockingQueue<Pair<Integer,byte[]>> writeQueue = new LinkedBlockingQueue<>();

        private final Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);

        private final UUID uid;

        private final Map<String,File> tmpFiles = new HashMap<>();

        private final Thread writerThread;

        private final Object lock = new Object();

        // ---------------------------------------------------

        private TmpFileWriter(final UUID uid) {
            this.uid = Preconditions.checkNotNull(uid);
            this.writerThread = new Thread(this);
        }

        public static TmpFileWriter start(final UUID uid) {
            final TmpFileWriter writer = new TmpFileWriter(uid);
            writer.writerThread.start();
            return writer;
        }

        // ---------------------------------------------------

        public void add(final int id,final byte[] buffer) {
            writeQueue.add(Pair.of(id, Preconditions.checkNotNull(buffer)));
        }

        public File getTmpFile(final String tmpFileName) {
            Preconditions.checkNotNull(tmpFileName);
            File file = null;
            try {
                synchronized(lock){
                    do {
                        if (tmpFiles.containsKey(tmpFileName))
                            file = tmpFiles.remove(tmpFileName);
                        else
                            lock.wait();
                    } while (file == null);
                }
            } catch (InterruptedException e) {}
            return file;
        }

        public UUID getUID() { return uid; }

        // ---------------------------------------------------

        @Override
        public void run() {
            while (isRunning) {
                try {
                    final Pair<Integer,byte[]> nextBuf = writeQueue.take();
                    final int id = nextBuf.getLeft();
                    final byte[] uncompressedBuf = nextBuf.getRight();
                    final byte[] compressedBuf = compressor.compress(uncompressedBuf);
                    final String fileName = uid.toString() + "-" + id;
                    final File tmpFile = File.createTempFile(uid.toString() + "-" + id, ".tmp");
                    final OutputStream os = new FileOutputStream(tmpFile);
                    os.write(compressedBuf.length);
                    os.write(compressedBuf);
                    os.flush();
                    os.close();
                    tmpFile.deleteOnExit();

                    synchronized (lock) {
                        tmpFiles.put(fileName, tmpFile);
                        lock.notifyAll();
                    }

                } catch(InterruptedException ie) {
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        // ---------------------------------------------------

        @Override
        public void deactivate() {
            isRunning = false;
            writerThread.interrupt();
        }
    }

    // ---------------------------------------------------

    public static final class TmpFileReader implements Runnable, Deactivatable {

        // ---------------------------------------------------

        private volatile boolean isRunning = true;

        private final BlockingQueue<Pair<Integer,byte[]>> readQueue = new LinkedBlockingQueue<>();

        private final BlockingQueue<byte[]> outputQueue = new LinkedBlockingQueue<>();

        private final Compressor compressor = Compressor.Factory.create(Compressor.CompressionType.LZ4_COMPRESSION);

        private final TmpFileWriter writer;

        private final Thread readerThread;

        // ---------------------------------------------------

        private TmpFileReader(final TmpFileWriter writer) {
            this.writer = Preconditions.checkNotNull(writer);
            readerThread = new Thread(this);
        }

        public static TmpFileReader start(TmpFileWriter writer) {
            final TmpFileReader reader = new TmpFileReader(writer);
            reader.readerThread.start();
            return reader;
        }
        // ---------------------------------------------------

        public void add(final int id,final byte[] buffer) {
            readQueue.add(Pair.of(id, Preconditions.checkNotNull(buffer)));
        }

        public byte[] get() {
            try {
                return outputQueue.take();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        // ---------------------------------------------------

        @Override
        public void run() {
            while (isRunning) {
                try {
                    final Pair<Integer,byte[]> nextBuf = readQueue.take();
                    final int id = nextBuf.getLeft();
                    final byte[] uncompressedBuf = nextBuf.getRight();
                    final File tmpFile = writer.getTmpFile(writer.getUID().toString() + "-" + id);
                    final InputStream is = new FileInputStream(tmpFile);
                    final int compressedLength = is.read();
                    final byte[] compressedBuf = new byte[compressedLength];
                    is.read(compressedBuf);
                    is.close();
                    outputQueue.add(compressor.decompress(compressedBuf, uncompressedBuf));
                } catch(InterruptedException ie) {
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        // ---------------------------------------------------

        @Override
        public void deactivate() {
            isRunning = false;
            readerThread.interrupt();
        }
    }

    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<byte[]> writeBuffers = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            writeBuffers.add(new byte[4096]);
        }

        final List<byte[]> readBuffers = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            readBuffers.add(new byte[4096]);
        }

        // ----------

        TmpFileWriter writer = TmpFileWriter.start(UUID.randomUUID());
        TmpFileReader reader = TmpFileReader.start(writer);

        // ----------

        for (int i = 0; i < writeBuffers.size(); ++i) {
            writer.add(i, writeBuffers.get(i));
        }

        // ----------

        for (int i = 0; i < readBuffers.size(); ++i) {
            reader.add(i, readBuffers.get(i));
        }

        // ----------

        for (int i = 0; i < readBuffers.size(); ++i) {
            reader.get();
            System.out.println("read " + i);
        }

        writer.deactivate();
        reader.deactivate();

        // ----------
    }
}
