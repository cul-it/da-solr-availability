package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LocationsTest {

  static VoyagerDBConnection testDB = null;
  static Connection voyagerTest = null;
  static Locations locations = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {

    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");
    voyagerTest = testDB.connection;
    locations = new Locations(voyagerTest);
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }

  @Test
  public void locationsPopulated() {

    edu.cornell.library.integration.voyager.Locations.Location l = locations.getByNumber(99);
    assertEquals("code: olin; number: 99; name: Olin Library; library: Olin Library; hoursCode: olin",l.toString());
    assertEquals("Olin Library",l.library);
    l = locations.getByCode("olin");
    assertEquals("olin",l.hoursCode);
  }

  @Test
  public void facetValues() {

    Set<String> facetValues = Locations.facetValues(locations.getByCode("mann"), "HK1234 .R56", null);
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Mann Library"));
    assertTrue(facetValues.contains("Mann Library > Main Collection"));
  }

  @Test
  public void facetValuesSuppressed() {

    Set<String> facetValues = Locations.facetValues(locations.getByCode("rmc,ts"), "HK1234 .R56", null);
    assertTrue(facetValues.isEmpty());
  }

  @Test
  public void facetValuesCallNumberSuffix() {

    Set<String> facetValues = Locations.facetValues(
        locations.getByCode("mann"), "HK1234 .R56 Curriculum Materials Collection", null);
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Mann Library"));
    assertTrue(facetValues.contains("Mann Library > Curriculum Materials Collection"));
  }
  
  @Test
  public void facetValuesCallNumberPrefix() {

    Set<String> facetValues = Locations.facetValues(
        locations.getByCode("olin"), "New & Noteworthy Books HK1234 .R56", null);
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Olin Library"));
    assertTrue(facetValues.contains("Olin Library > New & Noteworthy Books Shelf"));

    facetValues = Locations.facetValues(
        locations.getByCode("mann"), "ELLIS HK1234 .R56", null);
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Mann Library"));
    assertTrue(facetValues.contains("Mann Library > Ellis Collection"));

    // location and prefix mismatch 
    facetValues = Locations.facetValues(
        locations.getByCode("mann"), "New & Noteworthy Books HK1234 .R56", null);
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Mann Library"));
    assertTrue(facetValues.contains("Mann Library > Main Collection"));
  }

  @Test
  public void facetValuesHoldingsNote() {

    Set<String> facetValues = Locations.facetValues(
        locations.getByCode("afr"), "HK1234 .R56", "New Books Shelf");
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Africana Library"));
    assertTrue(facetValues.contains("Africana Library > New Books Shelf"));

    facetValues = Locations.facetValues(
        locations.getByCode("afr"), "HK1234 .R56 New Books Shelf",null);//not a holding note
    assertEquals(2,facetValues.size());
    assertTrue(facetValues.contains("Africana Library"));
    assertTrue(facetValues.contains("Africana Library > Main Collection"));
  }

}
