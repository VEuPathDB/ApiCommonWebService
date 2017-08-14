package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.ArrayList;
import java.util.List;

public class SequenceReverser {

    /**
     * @param args
     */
    public static void main(String[] args) {
        List<String> sequences = new ArrayList<String>();
        for(String arg : args) {
            sequences.add(reverse(arg));
        }
        for(int i = sequences.size() -1; i >= 0; i--) {
            System.out.println(sequences.get(i));
        }
    }
     
    private static String reverse(String sequence) {
        StringBuilder buffer = new StringBuilder();
        for(int i = sequence.length() - 1; i>=0; i--) {
            char ch = sequence.charAt(i);
            if (ch == 'A' || ch == 'a') buffer.append("T");
            else if (ch == 'T' || ch == 't') buffer.append("A");
            else if (ch == 'C' || ch == 'c') buffer.append("G");
            else if (ch == 'G' || ch == 'g') buffer.append("C");
            else {
                System.out.println("Unknown letter: '" + ch + "'");
                buffer.append(ch);
            }
        }
        return buffer.toString();
    }

}
