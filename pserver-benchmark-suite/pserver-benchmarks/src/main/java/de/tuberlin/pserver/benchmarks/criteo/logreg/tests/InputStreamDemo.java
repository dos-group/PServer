package de.tuberlin.pserver.benchmarks.criteo.logreg.tests;

public class InputStreamDemo {

    /*i = is.read();
      c = (char)i;
      while (c == ' ') {
          i = is.read();
          if (i == -1)
             return;
          c = (char) i;
      }

    /*public static final class SVMRecordParser {

        static int i;
        static char c;
        static int bi = 0;
        static char buf[] = new char[256];
        static StringBuilder sb = new StringBuilder();

        public static float parseSVMRecord(InputStream is, SVMRecord record, TLongFloatMap attributes) {
            float label = Float.NaN;
            try {
                // --------------------------------------
                // PARSE LABEL
                // --------------------------------------

                i = is.read();
                c = (char) i;
                if (i == -1)
                    throw new IllegalStateException();
                while ((c == '-' || c == '.' || Character.isDigit(c))) {
                    buf[bi++] = c;
                    i = is.read();
                    if (i == -1)
                        break;
                    c = (char) i;
                }

                sb.append(buf, 0, bi);
                label = Float.parseFloat(sb.toString());
                sb.setLength(0);
                bi = 0;

                // --------------------------------------
                // PARSE ENTRY
                // --------------------------------------
                while (c != '\n') {

                    // ---------------
                    // PARSE INDEX
                    // ---------------
                    if (c != ' ')
                        throw new IllegalStateException();
                    i = is.read();
                    c = (char) i;
                    if (i == -1)
                        throw new IllegalStateException();
                    while ((c == '-' || c == '.' || Character.isDigit(c))) {
                        buf[bi++] = c;
                        i = is.read();
                        if (i == -1)
                            break;
                        c = (char) i;
                    }
                    sb.append(buf, 0, bi);
                    long index = Long.parseLong(sb.toString());
                    sb.setLength(0);
                    bi = 0;

                    // ---------------
                    // PARSE VALUE
                    // ---------------
                    if (c != ':')
                        throw new IllegalStateException();
                    i = is.read();
                    c = (char) i;
                    if (i == -1)
                        throw new IllegalStateException();
                    while ((c == '-' || c == '.' || Character.isDigit(c))) {
                        buf[bi++] = c;
                        i = is.read();
                        if (i == -1)
                            break;
                        c = (char) i;
                    }
                    sb.append(buf, 0, bi);
                    index--;
                    attributes.put(index, Float.parseFloat(sb.toString()));
                    sb.setLength(0);
                    bi = 0;
                }

            } catch(Throwable t) {

                System.out.println("current row = " + record.getRow() + "\n"
                        + " | " + record.recordIter.file + "\n"
                        + " | " + record.recordIter.blockSeqID + "\n"
                        + " | " + t.getMessage());
                throw new IllegalStateException(t);
            }

            return label;
        }
    }*/

    public static void main(String[] args) throws Exception {

        //        => globalBlockCount = 774
        //        => blocks per host = 48
        //        => block rest = 6

        int a = 774;
        int b = 48;

        double r = 774.0 / 48.0;

        System.out.println("=> " + r);
    }
}


