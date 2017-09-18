package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.voyager.TestUtil.convertStreamToString;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class HoldingsTest {

  String expectedMarc1184953 =
  "000    00214nx  a2200097z  4500\n"+
  "001    1184953\n"+
  "004    969430\n"+
  "008    0005172u    8   4001uu   0000000\n"+
  "014 1  ‡a AED2310CU001\n"+
  "014 0  ‡9 001182083\n"+
  "852 00 ‡b ilr,anx ‡h HC59.7 ‡i .B16 1977 ‡x os=y\n";

  static Connection voyagerTest = null;
  static Connection voyagerLive = null;
  static Map<String,HoldingSet> examples;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException, IOException {

    // Connect to live Voyager database
/*    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
*/
    // Connect to test Voyager database
    Class.forName("org.sqlite.JDBC");
    voyagerTest = DriverManager.getConnection("jdbc:sqlite:src/test/resources/voyagerTest.db");

    // Load expected result JSON for tests
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("holdings_examples.json")){
      ObjectMapper mapper = new ObjectMapper();
      examples = mapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$" , ""),
          new TypeReference<HashMap<String,HoldingSet>>() {});
    }
  }

  @Test
  public void getHoldingByHoldingId() throws SQLException, IOException, XMLStreamException {
    HoldingSet holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1184953);
    assertEquals(examples.get("expectedJson1184953").toJson(),holding.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holding.get(1184953).date)).toString());
    assertEquals(expectedMarc1184953,holding.get(1184953).record.toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    assertEquals(examples.get("expectedJson9850688").toJson(),holding.toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holding.get(9850688).date)).toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 2202712);
    assertEquals(examples.get("expectedJson2202712").toJson(),holding.toJson());
    assertEquals("Tue Jul 14 13:03:11 EDT 2009",(new Date(1000L*holding.get(2202712).date)).toString());

    assertEquals(examples.get("expectedJson1131911").toJson(),
        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911).toJson());
    assertEquals(examples.get("expectedJson413836").toJson(),
        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 413836).toJson());
  }

  @Test
  public void getHoldingByBibId() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 969430);
    assertEquals(1,holdings.size());
    assertEquals(examples.get("expectedJson1184953").toJson(),holdings.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(1184953).date)).toString());
  }

  @Test
  public void roundTripHoldingThroughJson() throws SQLException, IOException, XMLStreamException {
    HoldingSet h1 = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    String j1 = h1.toJson();
    HoldingSet h2 = Holdings.extractHoldingsFromJson(j1);
    String j2 = h2.toJson();
    assertEquals(examples.get("expectedJson9850688").toJson(),j2);
  }

  @Test
  public void summarizeAvailability() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9975971);
    ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, 9975971);
    h.get(9975971).summarizeItemAvailability(i.getItems().get(9975971));
    assertEquals(examples.get("expectedJsonWithAvailability9975971").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911);
    i = Items.retrieveItemsByHoldingId(voyagerTest, 1131911);
    h.get(1131911).summarizeItemAvailability(i.getItems().get(1131911));
    assertEquals(examples.get("expectedJsonWithAvailability1131911").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4442869);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonWithAvailabilityELECTRICSHEEP").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1055);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(1055));
    }
    assertEquals(examples.get("expectedJsonMissing").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedMultivolMixedAvail").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 9628566);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonOnReserve").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2026746);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonPartialReserveHolding").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4546769);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonPartiallyInTempLocAndUnavail").toJson(),h.toJson());
  }
}
