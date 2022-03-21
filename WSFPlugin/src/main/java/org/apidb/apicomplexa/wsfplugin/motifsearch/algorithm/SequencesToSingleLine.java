package org.apidb.apicomplexa.wsfplugin.motifsearch.algorithm;

import java.io.*;

public class SequencesToSingleLine extends InputStream {

    public static void toSingleLine(FileReader fileReader, String fileOutput) throws Exception {
        final File outputFile = new File(fileOutput);
        try (BufferedReader in = new BufferedReader(fileReader);
             BufferedWriter out = new BufferedWriter(new FileWriter(outputFile))) {
            boolean first = true;
            String line;
            while ((line = in.readLine()) != null) {
                boolean defline = line.startsWith(">");
                if (defline) {
                    if (first) {
                        // beginning of file; don't need newline
                        first = false;
                    }
                    else {
                        // end of previous sequence; print newline
                        out.newLine();
                    }
                }
                out.write(line);
                if (defline) {
                    // newline at the end of deflines
                    out.newLine();
                }
            }
        }
    }

    @Override
    public int read() throws IOException {
        return 0;
    }
}
