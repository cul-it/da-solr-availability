package edu.cornell.library.integration.folio;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PODTests {

  @Test
  public void getHostFromUrlTest() {
    assertEquals("some.site.com",PODExporter.getHostFromUrl("http://some.site.com/hiya"));
    assertEquals("some.site.com",PODExporter.getHostFromUrl("https://some.site.com/hiya"));
    assertEquals("some.site.com",PODExporter.getHostFromUrl("http://some.site.com"));
    assertEquals("some.site.com",PODExporter.getHostFromUrl("https://some.site.com"));
  }
}
