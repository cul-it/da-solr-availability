package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.voyager.TestUtil.convertStreamToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.junit.AfterClass;
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

  static VoyagerDBConnection testDB = null;
  static Connection voyagerTest = null;
  static Connection voyagerLive = null;
  static Map<String,HoldingSet> examples;

  @BeforeClass
  public static void connect() throws SQLException, IOException {

    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");
    voyagerTest = testDB.connection;
//    voyagerLive = VoyagerDBConnection.getLiveConnection("database.properties");

    // Load expected result JSON for tests
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("holdings_examples.json")){
      ObjectMapper mapper = new ObjectMapper();
      examples = mapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$" , ""),
          new TypeReference<HashMap<String,HoldingSet>>() {});
    }
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }

  @Test
  public void getHoldingByHoldingId() throws SQLException, IOException, XMLStreamException {
    HoldingSet holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1184953);
    assertEquals(examples.get("expectedJson969430").get(1184953).toJson(),holding.get(1184953).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holding.get(1184953).date)).toString());
    assertEquals(this.expectedMarc1184953,holding.get(1184953).record.toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    assertEquals(examples.get("expectedJson9850688").toJson(),holding.toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holding.get(9850688).date)).toString());

    assertEquals(examples.get("expectedJson1131911").toJson(),
        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911).toJson());
    assertEquals(examples.get("expectedJson413836").toJson(),
        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 413836).toJson());
  }

  @Test
  public void getHoldingByBibId() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 969430);
    assertEquals(2,holdings.size());
    assertTrue( holdings.get(1184953).active);
    assertFalse(holdings.get(1184954).active);
    assertEquals(examples.get("expectedJson969430").toJson(),holdings.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(1184953).date)).toString());
  }

  @Test
  public void boundWithReference() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 833840);
    assertEquals(examples.get("expectedBib833840").toJson(),holdings.toJson());
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
    ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, 9975971, h.get(9975971).active);
    h.get(9975971).summarizeItemAvailability(i.getItems().get(9975971));
    assertEquals(examples.get("expectedJsonWithAvailability9975971").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911);
    i = Items.retrieveItemsByHoldingId(voyagerTest, 1131911, h.get(1131911).active);
    h.get(1131911).summarizeItemAvailability(i.getItems().get(1131911));
    assertEquals(examples.get("expectedJsonWithAvailability1131911").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4442869);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonWithAvailabilityELECTRICSHEEP").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1055);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(1055));
    }
    assertEquals(examples.get("expectedJsonMissing").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedMultivolMixedAvail").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 9628566);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonOnReserve").toJson(),h.toJson());

    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2026746);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonPartialReserveHolding").toJson(),h.toJson());
    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4546769);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonPartiallyInTempLocAndUnavail").toJson(),h.toJson());
    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 10023626);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJsonOnline").toJson(),h.toJson());
    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 1799377);
    for (int mfhdId : h.getMfhdIds()) {
      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedJson2202712").toJson(),h.toJson());
    assertEquals("++",h.get(2202712).callNumberSuffix);
  }

  @Test
  public void urlInsertIntoHoldings() throws SQLException, IOException, XMLStreamException {
    Set<Object> urlJsons = new HashSet<>();
    urlJsons.add("{\"providercode\":\"PRVAVX\",\"dbcode\":\"0D8\","+
       "\"description\":\"Full text available from SpringerLink ebooks"
       + " - Earth and Environmental Science (Contemporary) Connect to text.\","+
       "\"ssid\":\"ssj0001846622\","+
       "\"url\":\"http://proxy.library.cornell.edu/login?url=https://link.springer.com/openurl"
       + "?genre=book&isbn=978-3-319-63492-0\"}");
    urlJsons.add("{\"providercode\":\"PRVAVX\",\"dbcode\":\"FOYMO\","+
       "\"description\":\"Full text available from SpringerLINK ebooks - STM (2018) Connect to text.\","+
       "\"ssid\":\"ssj0001846622\","+
       "\"url\":\"http://proxy.library.cornell.edu/login?url=https://link.springer.com/10.1007/978-3-319-63492-0\"}");
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 10023626);
    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
    assertEquals(examples.get("expectedJsonOnlineWithLinks").toJson(),h.toJson());
  }

  @Test
  public void hathiLinkProducesFakeHoldings() throws SQLException, IOException, XMLStreamException {
    Set<Object> urlJsons = new HashSet<>();
    urlJsons.add("{\"description\":\"HathiTrust\",\"url\":\"http://hdl.handle.net/2027/coo.31924089590891\"}");
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4345125);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
    assertEquals(examples.get("expectedJsonPrintWithFakeHathiHolding").toJson(),h.toJson());
  }

  @Test
  public void linkWithInterestingHoldingRecord() throws SQLException, IOException, XMLStreamException {
    Set<Object> urlJsons = new HashSet<>();
    urlJsons.add(
        "{\"description\":\"HeinOnline Legal Classics Library Connect to full text. Access limited to authorized"
        + " subscribers.\","+
        "\"url\":\"http://proxy.library.cornell.edu/login?url=https://www.heinonline.org/HOL/Index"
        + "?index=beal/lreapcc&collection=bealL_beal\"}");
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 7187316);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
    assertEquals(examples.get("interestingOnlineHolding").toJson(),h.toJson());
  }

  @Test
  public void bothVoyagerAccessLinkAndHathiLink() throws SQLException, IOException, XMLStreamException {
    Set<Object> urlJsons = new HashSet<>();
    urlJsons.add("{\"url\":\"http://resolver.library.cornell.edu/moap/anw1478\"}");
    urlJsons.add("{\"description\":\"HathiTrust (multiple volumes)\","
                + "\"url\":\"http://catalog.hathitrust.org/Record/009586797\"}");
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2813334);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
    assertEquals(examples.get("bothVoyagerAccessLinkAndHathiLink").toJson(),h.toJson());
  }

  @Test
  public void onSiteUse() throws SQLException, IOException, XMLStreamException {
    int bib = 867;
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
  }

  @Test
  public void inProcess() throws SQLException, IOException, XMLStreamException {
    int bib = 9295667;
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedInProcess").toJson(),h.toJson());
  }

  @Test
  public void lost() throws SQLException, IOException, XMLStreamException {
    int bib = 4888514;
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedLost").toJson(),h.toJson());
  }

  @Test
  public void checkedOutReserve() throws SQLException, IOException, XMLStreamException {
    int bib = 1282748;
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedCheckedOutReserve").toJson(),h.toJson());
  }

  @Test
  public void recentIssues() throws SQLException, IOException, XMLStreamException {
    int bib = 369282;
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
    h.getRecentIssues(voyagerTest, null, bib);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    assertEquals(examples.get("expectedRecentItems").toJson(),h.toJson());
  }

  @Test
  public void donors() throws SQLException, IOException, XMLStreamException {
    int bib = 10604045;
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
    for (int mfhdId : h.getMfhdIds()) {
      assertNotNull( h.get(mfhdId).donors );
      assertEquals( 1, h.get(mfhdId).donors.size() );
      assertEquals(
          "Professor Ángel Sáenz-Badillos and Professor Judit Targarona Borrás", h.get(mfhdId).donors.get(0));
    }
  }

  @Test
  public void noCallNumberWithPrefix() throws SQLException, IOException, XMLStreamException {
    // 852 81 ‡b olin ‡k Newspapers ‡h No call number ‡z Subscription cancelled after 1996.
    HoldingSet holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 2354298);
    assertEquals("Newspapers",holding.get(2354298).call);
  }

  @Test
  public void partiallySuppressedHoldings() throws SQLException, IOException, XMLStreamException {
    {
      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 1449673);
      assertEquals(examples.get(
      "RMC and Annex copies active, not other Annex and Iron Mountain").toJson(),h.toJson());
    }
    {
      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 11764);
      assertEquals(examples.get("Annex copy active, not Mann").toJson(),h.toJson());
    }
    {
      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 34985);
      assertEquals(examples.get(
      "Annex copy, rare Annex copy active, not Microfilm").toJson(),h.toJson());
    }
    {
      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 5487364);
      assertEquals(examples.get("Olin copy active, not RMC copy").toJson(),h.toJson());
    }
    {
      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 301608);
      assertEquals(examples.get(
      "RMC copy, Annex copy & microfilm active, Iron Mountain & master microfilm inactive")
      .toJson(),h.toJson());
    }
  }

  @Test
  public void areTheseMathCallNumbers() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9975971); //AC8.5 .G74 2016
    assertFalse(h.hasMathCallNumber());
    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 2034612); //QA1
    assertTrue(h.hasMathCallNumber());
    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1420558); //QA75.A1 Z632
    assertFalse(h.hasMathCallNumber());
    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1468093); //QA155 .A33 1981
    assertTrue(h.hasMathCallNumber());
    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9598247); //QA155.7.E4 B37 2015
    assertFalse(h.hasMathCallNumber());
  }

}
