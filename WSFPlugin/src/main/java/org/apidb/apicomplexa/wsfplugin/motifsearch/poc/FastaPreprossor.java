package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FastaPreprossor {

    public static void toSingleLine(FileReader fileReader, String fileOutput) throws Exception {
        final File outputFile = new File(fileOutput);
        try (BufferedReader in = new BufferedReader(fileReader);
             BufferedWriter out = new BufferedWriter(new FileWriter(outputFile))) {
            boolean first = true;
            String line;
            int bytes = 0;
            while ((line = in.readLine()) != null) {
                boolean defline = line.startsWith(">");
                if (defline) {
                    if (first) {
                        // beginning of file; don't need newline
                        first = false;
                        bytes = 0;
                    } else {
                        // end of previous sequence; print newline
                        bytes += line.getBytes(StandardCharsets.UTF_8).length;
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
}
