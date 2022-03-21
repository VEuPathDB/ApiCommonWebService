package org.apidb.apicomplexa.wsfplugin.motifsearch.algorithm;

import org.apidb.apicomplexa.wsfplugin.motifsearch.exception.MotifTooLongException;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;
import java.util.regex.Pattern;

public class BufferedDnaMotifFinderTest {

    /**
     * Tests a max-length sequence that will appear in the overlap window and verifies that the match is still picked up.
     */
    @Test
    public void testMatchBetweenWindows() throws Exception {
        final StringReader stringReader = new StringReader("XXXXXXXXXXXBBAAAFFXXXX");
        final List<MotifMatch> matches = BufferedDnaMotifFinder.match(stringReader,
                Pattern.compile("AAA"), 2, 4,3);
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(13, matches.get(0).getStartPos());
        Assert.assertEquals(16, matches.get(0).getEndPos());
        Assert.assertEquals("BB", matches.get(0).getLeadingContext());
        Assert.assertEquals("FF", matches.get(0).getTrailingContext());
        Assert.assertEquals("AAA", matches.get(0).getMatch());
    }

    @Test
    public void testMatchAtEnd() throws Exception {
        final StringReader stringReader = new StringReader("ATCGATGGCCTATXXXX");
        final List<MotifMatch> matches = BufferedDnaMotifFinder.match(stringReader,
                Pattern.compile("XXXX"), 2, 4 ,4);
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(13, matches.get(0).getStartPos());
        Assert.assertEquals(17, matches.get(0).getEndPos());
        Assert.assertEquals("AT", matches.get(0).getLeadingContext());
        Assert.assertEquals("", matches.get(0).getTrailingContext());
        Assert.assertEquals("XXXX", matches.get(0).getMatch());
    }

    @Test
    public void testMatchAtBeginning() throws Exception {
        final StringReader stringReader = new StringReader("XXXXATCGTCGAAAAGCGCTA");
        final List<MotifMatch> matches = BufferedDnaMotifFinder.match(stringReader,
                Pattern.compile("XXXX"), 2, 4 ,4);
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(0, matches.get(0).getStartPos());
        Assert.assertEquals(4, matches.get(0).getEndPos());
        Assert.assertEquals("", matches.get(0).getLeadingContext());
        Assert.assertEquals("AT", matches.get(0).getTrailingContext());
        Assert.assertEquals("XXXX", matches.get(0).getMatch());
    }


    @Test(expected = MotifTooLongException.class)
    public void testMatchExceedsMaxLength() throws Exception {
        final StringReader stringReader = new StringReader("XXXXXXXXXX");
        final List<MotifMatch> matches = BufferedDnaMotifFinder.match(stringReader,
                Pattern.compile("X*"), 2, 4 ,4);
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(0, matches.get(0).getStartPos());
        Assert.assertEquals(4, matches.get(0).getEndPos());
        Assert.assertEquals("", matches.get(0).getLeadingContext());
        Assert.assertEquals("AT", matches.get(0).getTrailingContext());
        Assert.assertEquals("XXXX", matches.get(0).getMatch());
    }
}
