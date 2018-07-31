package edu.cornell.library.integration.solr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CallNumberSortTest {

  @Test
  public void testCallNumberSortForm() {

    assertEquals("ml 000000410 m 000000619 g 000000056 000002012",
        CallNumberSortFilter.callNumberSortForm("ML410.M619 G56 2012"));
    assertEquals("a 000000123 012345678.1",
        CallNumberSortFilter.callNumberSortForm("A123 12345678.1"));
    assertEquals(
        CallNumberSortFilter.callNumberSortForm("AB12.3R18."),
        CallNumberSortFilter.callNumberSortForm("AB 12.3 .R 18"));
    assertEquals("r 000000012.0000000005",
        CallNumberSortFilter.callNumberSortForm("R12.0000000005."));
    assertEquals("r 000000001.2.3.4",
        CallNumberSortFilter.callNumberSortForm("R1.2.3.4"));

  }

}
