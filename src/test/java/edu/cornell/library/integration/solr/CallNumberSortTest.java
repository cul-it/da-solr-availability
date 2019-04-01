package edu.cornell.library.integration.solr;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class CallNumberSortTest {

  @Test
  public void testCallNumberSortForm() {

    assertEquals("ml 000000410 m 619 g 56 2012",
        CallNumberSortFilter.sortForm("ML410.M619 G56 2012",null));
    //skeletal call numbers:
    assertEquals("ml m 619 g 56 2012",
        CallNumberSortFilter.sortForm("ML M619 G56 2012",null));
    assertEquals("ml m 619 g 56 2012",
        CallNumberSortFilter.sortForm("ML .M619 G56 2012",null));

    assertEquals("a 000000123 12345678.1",
        CallNumberSortFilter.sortForm("A123 12345678.1",null));
    assertEquals(
        CallNumberSortFilter.sortForm("AB12.3R18.",null),
        CallNumberSortFilter.sortForm("AB 12.3 .R 18",null));
    assertEquals("r 000000012.0000000005",
        CallNumberSortFilter.sortForm("R12.0000000005.",null));
    assertEquals("r 000000001.2.3.4",
        CallNumberSortFilter.sortForm("R1.2.3.4",null));

    assertEquals("000000001 1 1",
        CallNumberSortFilter.sortForm("1-1-1",null));
  }

  @Test
  public void callnumberprefixes() {
    List<String> prefixes = Arrays.asList("oversize","rare books","new books","a d white","icelandic");

    assertEquals("abc 000000123.1 r 15 2018",
        CallNumberSortFilter.sortForm("Oversize ABC123.1 .R15 2018",prefixes));

    assertEquals("z 000002557 d 57",
        CallNumberSortFilter.sortForm("A.D. White, Icelandic Z2557 .D57",prefixes));

    assertEquals(
        CallNumberSortFilter.sortForm("A.D. White, Icelandic Z2557 .D57",prefixes),
        CallNumberSortFilter.sortForm("A.  D.White; Icelandic Z2557 .D57",prefixes));

    // Failing to match the prefix causes main classification number to sort alphabetically instead of numerically
    assertEquals("rare a 12",CallNumberSortFilter.sortForm("Rare A12",prefixes));

    assertEquals("bx 000001935 a 23 1959",
        CallNumberSortFilter.sortForm("++ Oversize BX1935 .A23 1959",prefixes));

    assertEquals("ice 000000001 w 131 1740",
        CallNumberSortFilter.sortForm("Icelandic IcE1W131 1740 +", prefixes));
  }
}
