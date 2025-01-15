package edu.cornell.library.integration.folio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.folio.Locations.Location;
import edu.cornell.library.integration.voyager.VoyagerDBConnection;

public class LocationsTest {

  static VoyagerDBConnection testDB = null;
  static Locations locations = null;
  static OkapiClient testOkapiClient = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {

    testOkapiClient = new StaticOkapiClient();
    locations = new Locations(testOkapiClient);

    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");

  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }

  @Test
  public void locationsPopulated() {

    Location l = locations.getByCode("olin");
    assertEquals("Olin Library",l.library);    assertEquals("code: olin; name: Olin Library; library: Olin Library; hoursCode: olinuris",l.toString());

    assertEquals("olinuris",l.hoursCode);
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
    assertTrue(facetValues.contains("Olin Library > New & Noteworthy"));

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
    System.out.println(locations.getByCode("afr").toString());
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
