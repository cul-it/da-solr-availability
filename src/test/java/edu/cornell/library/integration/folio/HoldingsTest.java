package edu.cornell.library.integration.folio;

import static edu.cornell.library.integration.db_test.TestUtil.loadResourceFile;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;

public class HoldingsTest extends DbBaseTest {

  String expectedMarc1184953 =
  "000    00214nx  a2200097z  4500\n"+
  "001    1184953\n"+
  "004    969430\n"+
  "008    0005172u    8   4001uu   0000000\n"+
  "014 1  ‡a AED2310CU001\n"+
  "014 0  ‡9 001182083\n"+
  "852 00 ‡b ilr,anx ‡h HC59.7 ‡i .B16 1977 ‡x os=y\n";

  static Connection testConnection = null;
//  static Connection liveConnection = null;
  static Map<String,HoldingSet> examples;
  static Locations locations = null;
  static ReferenceData holdingsNoteTypes = null;
  static ReferenceData callNumberTypes = null;
  static ReferenceData materialTypes = null;
  static ReferenceData itemNoteTypes = null;
  static OkapiClient testOkapiClient = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {
    setup();
    testConnection = getConnection();

    // Load expected result JSON for tests
    ObjectMapper mapper = new ObjectMapper();
    examples = mapper.readValue(loadResourceFile("folio_holdings_examples.json").replaceAll("(?m)^#.*$" , ""),
        new TypeReference<HashMap<String,HoldingSet>>() {});

    testOkapiClient = new StaticOkapiClient();
    locations = new Locations(testOkapiClient);
    Items.initialize(testOkapiClient, locations);
    ServicePoints.initialize(testOkapiClient);
    LoanTypes.initialize(testOkapiClient);
    holdingsNoteTypes = new ReferenceData(testOkapiClient, "/holdings-note-types", "name");
    callNumberTypes = new ReferenceData(testOkapiClient, "/call-number-types", "name");
    materialTypes = new ReferenceData(testOkapiClient, "/material-types", "name");
    itemNoteTypes = new ReferenceData(testOkapiClient, "/item-note-types", "name");
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    if (testConnection != null) {
      testConnection.close();
    }
  }

//  @Test
//  public void getHoldingByHoldingId() throws SQLException, IOException, XMLStreamException {
//    
//    HoldingSet holding = Holdings.retrieveHoldingsByInstanceHrid(testConnection, 1184953);
//    assertEquals(examples.get("expectedJson969430").get(1184953).toJson(),holding.get(1184953).toJson());
//    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holding.get(1184953).date)).toString());
//    assertEquals(this.expectedMarc1184953,holding.get(1184953).record.toString());
//
//    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
//    assertEquals(examples.get("expectedJson9850688").toJson(),holding.toJson());
//    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holding.get(9850688).date)).toString());
//
//    assertEquals(examples.get("expectedJson1131911").toJson(),
//        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911).toJson());
//    assertEquals(examples.get("expectedJson413836").toJson(),
//        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 413836).toJson());
//  }

  // https://da-folio-solr.library.cornell.edu/solr/blacklight/select?qt=document&id=969430&fl=holdings_json
  /*
   * Connection inventory, Locations locations, ReferenceData holdingsNoteTypes,
      ReferenceData callNumberTypes, String instanceHrid
   */
  @Test
  public void retrieveHoldingsByInstanceHrid() throws SQLException, IOException {
    HoldingSet hs = Holdings.retrieveHoldingsByInstanceHrid(testConnection, locations, holdingsNoteTypes, callNumberTypes, "969430");
    System.out.println(hs.toJson());
    assertEquals(2,hs.size());
    assertTrue(hs.get("263a48a0-7067-4a3a-a034-8275ac46850f").active);
    assertFalse(hs.get("fc2579de-91b2-4e70-8205-3cab8e04b7aa").active);
    assertEquals(examples.get("expectedJson-263a48a0-7067-4a3a-a034-8275ac46850f").toJson(),hs.toJson());
    /*
     * Frances found bug in the Holding date - first letter should be lower case for UpdatedDate, CreatedDate
     * When we make the change, the date parsing fails
     * Fix that issue before testing dates
     * 
     * assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get("263a48a0-7067-4a3a-a034-8275ac46850f").date)).toString());
     */
  }

  // "items":{"count":1,"unavail":[{"id":"352f3f3e-af4a-4642-a585-fdbc96e2ab8a","enum":"Bound with","status":{"status":"Checked out","due":1693108799}}]}
//  @Test
//  public void boundWithReference() throws SQLException, IOException {
//    HoldingSet holdings = Holdings.retrieveHoldingsByInstanceHrid(testConnection, locations, holdingsNoteTypes, callNumberTypes, "833840");
//    // HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 833840);
//    assertEquals(examples.get("expectedJson-9f82a250-ebf9-4d65-b7ff-c7c145edab47").toJson(),holdings.toJson());
//  }

  @Test
  public void roundTripHoldingThroughJson() throws SQLException, IOException {
    HoldingSet h1 = Holdings.retrieveHoldingsByInstanceHrid(testConnection, locations, holdingsNoteTypes, callNumberTypes, "9850688");
    String j1 = h1.toJson();
    HoldingSet h2 = Holdings.extractHoldingsFromJson(j1);
    String j2 = h2.toJson();
    System.out.println(j2);
    assertEquals(examples.get("expectedJson-66fe0529-6316-4377-81ac-0d776b0c50ab").toJson(),j2);
  }

  @Test
  public void summarizeAvailability() throws SQLException, IOException {
    HoldingSet hs = Holdings.retrieveHoldingsByInstanceHrid(testConnection, locations, holdingsNoteTypes, callNumberTypes, "10055679");
    Holding h = hs.get("f26ba953-b4b6-486b-8113-1cffc3f3c3f8");
    ItemList il = Items.retrieveItemsForHoldings(testOkapiClient, testConnection, "10055679", hs);
    Map<String, TreeSet<Item>> itemSet = il.getItems();
    TreeSet<Item> itemTreeSet = itemSet.get("f26ba953-b4b6-486b-8113-1cffc3f3c3f8");
    Instant availabilityInstant = h.summarizeItemAvailability(itemTreeSet);
    assertNull(availabilityInstant);
    assertEquals(examples.get("expectedJson-f26ba953-b4b6-486b-8113-1cffc3f3c3f8").toJson(),hs.toJson());

    hs = Holdings.retrieveHoldingsByInstanceHrid(testConnection, locations, holdingsNoteTypes, callNumberTypes, "2805041");
    h = hs.get("fdac516c-91eb-438d-b784-0b99b19769d4");
    il = Items.retrieveItemsForHoldings(testOkapiClient, testConnection, "2805041", hs);
    itemSet = il.getItems();
    for (String id : itemSet.keySet()) {
      itemTreeSet = itemSet.get(id);
      availabilityInstant = h.summarizeItemAvailability(itemTreeSet);
    }
    assertEquals(examples.get("expectedJson-1cbded88-05d9-460d-b930-0427e4718e1b").toJson(),hs.toJson());

    hs = Holdings.retrieveHoldingsByInstanceHrid(testConnection, locations, holdingsNoteTypes, callNumberTypes, "4442869");
    il = Items.retrieveItemsForHoldings(testOkapiClient, testConnection, "4442869", hs);
    for (String hId : hs.getUuids()) {
      hs.get(hId).summarizeItemAvailability(il.getItems().get(hId));
    }
    assertEquals(examples.get("expectedJson-7d7cca49-d86c-425c-820e-d46b9f2ec998").toJson(),hs.toJson());
  }
//
//    h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1055);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(1055));
//    }
//    assertEquals(examples.get("expectedJsonMissing").toJson(),h.toJson());
//
//    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedMultivolMixedAvail").toJson(),h.toJson());
//
//    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 9628566);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedJsonOnReserve").toJson(),h.toJson());
//
//    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2026746);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedJsonPartialReserveHolding").toJson(),h.toJson());
//    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4546769);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedJsonPartiallyInTempLocAndUnavail").toJson(),h.toJson());
//    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 10023626);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedJsonOnline").toJson(),h.toJson());
//    h = Holdings.retrieveHoldingsByBibId(voyagerTest, 1799377);
//    for (int mfhdId : h.getMfhdIds()) {
//      i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedJson2202712").toJson(),h.toJson());
//    assertEquals("++",h.get(2202712).callNumberSuffix);
//  }
//
//  @Test
//  public void urlInsertIntoHoldings() throws SQLException, IOException, XMLStreamException {
//    Set<Object> urlJsons = new HashSet<>();
//    urlJsons.add("{\"providercode\":\"PRVAVX\",\"dbcode\":\"0D8\","+
//       "\"description\":\"Full text available from SpringerLink ebooks"
//       + " - Earth and Environmental Science (Contemporary) Connect to text.\","+
//       "\"ssid\":\"ssj0001846622\","+
//       "\"url\":\"http://proxy.library.cornell.edu/login?url=https://link.springer.com/openurl"
//       + "?genre=book&isbn=978-3-319-63492-0\"}");
//    urlJsons.add("{\"providercode\":\"PRVAVX\",\"dbcode\":\"FOYMO\","+
//       "\"description\":\"Full text available from SpringerLINK ebooks - STM (2018) Connect to text.\","+
//       "\"ssid\":\"ssj0001846622\","+
//       "\"url\":\"http://proxy.library.cornell.edu/login?url=https://link.springer.com/10.1007/978-3-319-63492-0\"}");
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 10023626);
//    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
//    assertEquals(examples.get("expectedJsonOnlineWithLinks").toJson(),h.toJson());
//  }
//
//  @Test
//  public void hathiLinkProducesFakeHoldings() throws SQLException, IOException, XMLStreamException {
//    Set<Object> urlJsons = new HashSet<>();
//    urlJsons.add("{\"description\":\"HathiTrust\",\"url\":\"http://hdl.handle.net/2027/coo.31924089590891\"}");
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4345125);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
//    assertEquals(examples.get("expectedJsonPrintWithFakeHathiHolding").toJson(),h.toJson());
//  }
//
//  @Test
//  public void linkWithInterestingHoldingRecord() throws SQLException, IOException, XMLStreamException {
//    Set<Object> urlJsons = new HashSet<>();
//    urlJsons.add(
//        "{\"description\":\"HeinOnline Legal Classics Library Connect to full text. Access limited to authorized"
//        + " subscribers.\","+
//        "\"url\":\"http://proxy.library.cornell.edu/login?url=https://www.heinonline.org/HOL/Index"
//        + "?index=beal/lreapcc&collection=bealL_beal\"}");
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 7187316);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
//    assertEquals(examples.get("interestingOnlineHolding").toJson(),h.toJson());
//  }
//
//  @Test
//  public void bothVoyagerAccessLinkAndHathiLink() throws SQLException, IOException, XMLStreamException {
//    Set<Object> urlJsons = new HashSet<>();
//    urlJsons.add("{\"url\":\"http://resolver.library.cornell.edu/moap/anw1478\"}");
//    urlJsons.add("{\"description\":\"HathiTrust (multiple volumes)\","
//                + "\"url\":\"http://catalog.hathitrust.org/Record/009586797\"}");
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2813334);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    Holdings.mergeAccessLinksIntoHoldings(h, urlJsons);
//    assertEquals(examples.get("bothVoyagerAccessLinkAndHathiLink").toJson(),h.toJson());
//  }
//
//  @Test
//  public void onSiteUse() throws SQLException, IOException, XMLStreamException {
//    int bib = 867;
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//  }
//
//  @Test
//  public void inProcess() throws SQLException, IOException, XMLStreamException {
//    int bib = 9295667;
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedInProcess").toJson(),h.toJson());
//  }
//
//  @Test
//  public void lost() throws SQLException, IOException, XMLStreamException {
//    int bib = 4888514;
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedLost").toJson(),h.toJson());
//  }
//
//  @Test
//  public void checkedOutReserve() throws SQLException, IOException, XMLStreamException {
//    int bib = 1282748;
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedCheckedOutReserve").toJson(),h.toJson());
//  }
//
//  @Test
//  public void recentIssues() throws SQLException, IOException, XMLStreamException {
//    int bib = 369282;
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
//    h.getRecentIssues(voyagerTest, null, bib);
//    for (int mfhdId : h.getMfhdIds()) {
//      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
//      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
//    }
//    assertEquals(examples.get("expectedRecentItems").toJson(),h.toJson());
//  }
//
//  @Test
//  public void donors() throws SQLException, IOException, XMLStreamException {
//    int bib = 10604045;
//    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, bib);
//    for (int mfhdId : h.getMfhdIds()) {
//      assertNotNull( h.get(mfhdId).donors );
//      assertEquals( 1, h.get(mfhdId).donors.size() );
//      assertEquals(
//          "Professor Ángel Sáenz-Badillos and Professor Judit Targarona Borrás", h.get(mfhdId).donors.get(0));
//    }
//  }
//
//  @Test
//  public void noCallNumberWithPrefix() throws SQLException, IOException, XMLStreamException {
//    // 852 81 ‡b olin ‡k Newspapers ‡h No call number ‡z Subscription cancelled after 1996.
//    HoldingSet holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 2354298);
//    assertEquals("Newspapers",holding.get(2354298).call);
//  }
//
//  @Test
//  public void partiallySuppressedHoldings() throws SQLException, IOException, XMLStreamException {
//    {
//      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 1449673);
//      assertEquals(examples.get(
//      "RMC and Annex copies active, not other Annex and Iron Mountain").toJson(),h.toJson());
//    }
//    {
//      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 11764);
//      assertEquals(examples.get("Annex copy active, not Mann").toJson(),h.toJson());
//    }
//    {
//      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 34985);
//      assertEquals(examples.get(
//      "Annex copy, rare Annex copy active, not Microfilm").toJson(),h.toJson());
//    }
//    {
//      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 5487364);
//      assertEquals(examples.get("Olin copy active, not RMC copy").toJson(),h.toJson());
//    }
//    {
//      HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 301608);
//      assertEquals(examples.get(
//      "RMC copy, Annex copy & microfilm active, Iron Mountain & master microfilm inactive")
//      .toJson(),h.toJson());
//    }
//  }

}
