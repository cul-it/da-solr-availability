package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Items.ItemList;
import edu.cornell.library.integration.voyager.VoyagerDBConnection;

public class CallNumberBrowseTest {

  static VoyagerDBConnection testDB = null;
  static Connection voyagerTest = null;
  static Connection inventory = null;
//  static Connection voyagerLive = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }

    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");
    voyagerTest = testDB.connection;
    inventory = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
//  voyagerLive = VoyagerDBConnection.getLiveConnection("database.properties");
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }

  @Test
  public void multipleCopies() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 4442869);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "4442869");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);

    assertEquals(3,docs.size());
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">4442869</field>"
        + "<field name=\"cite_preescaped_display\"></field>"
        + "<field name=\"id\">4442869.1</field>"
        + "<field name=\"callnum_sort\">Rare Books PS3554.I3 D6 1996 0 4442869.1</field>"
        + "<field name=\"callnum_display\">Rare Books PS3554.I3 D6 1996</field>"
        + "<field name=\"lc_b\">true</field>"
        + "<field name=\"availability_json\">{\"available\":true,"
        +    "\"availAt\":{\"Kroch Library Rare &amp; Manuscripts (Non-Circulating)\":\"\"}}</field>"
        + "<field name=\"classification_display\">P - Language &amp; Literature "
        +    "&gt; PS - Americal Literature &gt; PS1-3576 - American literature "
        +    "&gt; PS700-3576 - Individual authors &gt; PS3550-3576 - 1961-2000</field>"
        + "<field name=\"location\">Kroch Library Rare &amp; Manuscripts</field>"
        + "<field name=\"location\">Kroch Library Rare &amp; Manuscripts &gt; Main Collection</field>"
        + "<field name=\"online\">At the Library</field></doc>",
        ClientUtils.toXML(docs.get(0)));
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">4442869</field>"
        + "<field name=\"cite_preescaped_display\"></field>"
        + "<field name=\"id\">4442869.2</field>"
        + "<field name=\"callnum_sort\">PS3554.I3 D6 1996 0 4442869.2</field>"
        + "<field name=\"callnum_display\">PS3554.I3 D6 1996</field>"
        + "<field name=\"lc_b\">true</field>"
        + "<field name=\"availability_json\">{\"available\":true,"
        +    "\"availAt\":{\"Olin Library\":\"\","
        +                 "\"ILR Library (Ives Hall)\":\"\","
        +                 "\"Mann Library\":\"\"}}</field>"
        + "<field name=\"classification_display\">P - Language &amp; Literature "
        +    "&gt; PS - Americal Literature &gt; PS1-3576 - American literature "
        +    "&gt; PS700-3576 - Individual authors &gt; PS3550-3576 - 1961-2000</field>"
        + "<field name=\"location\">Olin Library</field>"
        + "<field name=\"location\">Olin Library &gt; Main Collection</field>"
        + "<field name=\"location\">ILR Library</field>"
        + "<field name=\"location\">ILR Library &gt; Main Collection</field>"
        + "<field name=\"location\">Mann Library</field>"
        + "<field name=\"location\">Mann Library &gt; Main Collection</field>"
        + "<field name=\"online\">At the Library</field></doc>",
        ClientUtils.toXML(docs.get(1)));
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">4442869</field>"
        + "<field name=\"cite_preescaped_display\"></field>"
        + "<field name=\"id\">4442869.3</field>"
        + "<field name=\"callnum_sort\">PS3554.I3 D6x 1996 0 4442869.3</field>"
        + "<field name=\"callnum_display\">PS3554.I3 D6x 1996</field>"
        + "<field name=\"lc_b\">true</field>"
        + "<field name=\"availability_json\">{\"available\":true,"
        +     "\"availAt\":{\"Library Annex\":\"\"},"
        +     "\"unavailAt\":{\"Olin Library\":\"\"}}</field>"
        + "<field name=\"classification_display\">P - Language &amp; Literature "
        +    "&gt; PS - Americal Literature &gt; PS1-3576 - American literature "
        +    "&gt; PS700-3576 - Individual authors &gt; PS3550-3576 - 1961-2000</field>"
        + "<field name=\"location\">Olin Library</field>"
        + "<field name=\"location\">Olin Library &gt; Main Collection</field>"
        + "<field name=\"location\">Library Annex</field>"
        + "<field name=\"online\">At the Library</field></doc>",
        ClientUtils.toXML(docs.get(2)));

  }

  @Test
  public void serial() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "329763");
    mainDoc.addField("lc_callnum_full", "Q1 .N2");
    mainDoc.addField("lc_bib_display", "Q1 .N2");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);

    assertEquals(2,docs.size());
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">329763</field>"
        + "<field name=\"cite_preescaped_display\"></field>"
        + "<field name=\"id\">329763.1</field>"
        + "<field name=\"callnum_sort\">Q1 .N282 0 329763.1</field>"
        + "<field name=\"callnum_display\">Q1 .N282</field>"
        + "<field name=\"lc_b\">true</field>"
        + "<field name=\"availability_json\">{\"available\":true,\"availAt\":{\"Library Annex\":\"\"}}</field>"
        + "<field name=\"classification_display\">Q - Science &gt; Q - Science (General) "
        +    "&gt; Q1-295 - General</field>"
        + "<field name=\"location\">Library Annex</field>"
        + "<field name=\"online\">At the Library</field></doc>",
        ClientUtils.toXML(docs.get(0)));
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">329763</field>"
        + "<field name=\"cite_preescaped_display\"></field>"
        + "<field name=\"id\">329763.2</field>"
        + "<field name=\"callnum_sort\">Q1 .N2 0 329763.2</field>"
        + "<field name=\"callnum_display\">Q1 .N2</field>"
        + "<field name=\"lc_b\">true</field>"
        + "<field name=\"availability_json\">{\"available\":true,\"availAt\":"
        +    "{\"Veterinary Library (Schurman Hall)\":\"\"}}</field>"
        + "<field name=\"classification_display\">Q - Science &gt; Q - Science (General) "
        +    "&gt; Q1-295 - General</field>"
        + "<field name=\"location\">Veterinary Library</field>"
        + "<field name=\"location\">Veterinary Library &gt; Main Collection</field>"
        + "<field name=\"online\">At the Library</field></doc>",
        ClientUtils.toXML(docs.get(1)));

  }

  @Test
  public void availableToPurchase() throws SQLException, IOException, XMLStreamException {

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 10005850);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "10005850");
    mainDoc.addField("lc_bib_display", "TL4030 .P454 2017");
    mainDoc.addField("isbn_display", "9780262035873 (hardcover : alk. paper)");
    mainDoc.addField("oclc_id_display", "959034140");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);
    String expected =
    "<doc boost=\"1.0\">"
    + "<field name=\"isbn_display\">9780262035873 (hardcover : alk. paper)</field>"
    + "<field name=\"oclc_id_display\">959034140</field>"
    + "<field name=\"bibid\">10005850</field>"
    + "<field name=\"cite_preescaped_display\"></field>"
    + "<field name=\"id\">10005850.1</field>"
    + "<field name=\"callnum_sort\">TL4030 .P454 2017 0 10005850.1</field>"
    + "<field name=\"callnum_display\">TL4030 .P454 2017</field>"
    + "<field name=\"lc_b\">true</field>"
    + "<field name=\"availability_json\">{\"availAt\":{\"Available for the Library to Purchase\":\"\"}}</field>"
    + "<field name=\"classification_display\">T - Technology &gt; TL - Motor Vehicles, Aeronautics, Astronautics"
    +    " &gt; TL787-4050 - Astronautics.  Space travel</field></doc>";
    assertEquals(1,docs.size());
    assertEquals(expected,ClientUtils.toXML(docs.get(0)));

  }

  @Test
  public void bibliographicData() throws SQLException, IOException, XMLStreamException {

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 9520154);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }


    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "9520154");
    mainDoc.addField("format", "Book");
    mainDoc.addField("fulltitle_display", "Shanghai yi shu ping lun = Shanghai art review");
    mainDoc.addField("fulltitle_vern_display", "上海艺术评论");
    mainDoc.addField("publisher_display", "《上海艺术评论》编辑部 / \"Shanghai yi shu ping lun\" bian ji bu");
    mainDoc.addField("pub_date_display", "2016年2月- 2016 nian 2 yue-");

    assertEquals("<b>上海艺术评论 &#x2F; Shanghai yi shu ping lun = Shanghai art review.</b>"
        + " 《上海艺术评论》编辑部 &#x2F; &quot;Shanghai yi shu ping lun&quot; bian ji bu, 2016年2月- 2016 nian 2 yue-.",
        CallNumberBrowse.generateBrowseDocuments(
            inventory, mainDoc, holdings).get(0).getFieldValue("cite_preescaped_display"));

    holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 301608);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "301608");
    mainDoc.addField("format", "Journal/Periodical");
    mainDoc.addField("fulltitle_display", "A biometrical study of characters in maize");
    mainDoc.addField("author_display", "Wolfe, Thomas Kennerly, 1892-");
    mainDoc.addField("pub_date_display", "1921");

    assertEquals("Wolfe, Thomas Kennerly, 1892- <b>A biometrical study of characters in maize.</b> 1921.",
        CallNumberBrowse.generateBrowseDocuments(
            inventory, mainDoc, holdings).get(0).getFieldValue("cite_preescaped_display"));

    holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 3212531);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "3212531");
    mainDoc.addField("format", "Book");
    mainDoc.addField("fulltitle_display", "The white side of a black subject : a vindication of the Afro-American"
        + " race : from the landing of slaves at St. Augustine, Florida, in 1565, to the present time");
    mainDoc.addField("author_display", "Wood, Norman B. (Norman Barton), 1857-1933.");
    mainDoc.addField("publisher_display", "American Pub. House");
    mainDoc.addField("pub_date_display", "1897");

    assertEquals("Wood, Norman B. (Norman Barton), 1857-1933."
        + " <b>The white side of a black subject : a vindication of the Afro-American race :"
        + " from the landing of slaves at St. Augustine, Florida, in 1565, to the present time.</b>"
        + " American Pub. House, 1897.",
        CallNumberBrowse.generateBrowseDocuments(
            inventory, mainDoc, holdings).get(0).getFieldValue("cite_preescaped_display"));


    holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 10663989);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "10663989");
    mainDoc.addField("format", "Book");
    mainDoc.addField("fulltitle_display", "Veterinary oral and maxillofacial pathology");
    mainDoc.addField("author_display", "Murphy, Brian G., 1966- author");
    mainDoc.addField("publisher_display", "Wiley-Blackwell");
    mainDoc.addField("pub_date_display", "2020");
    mainDoc.addField("lc_callnum_full", "SF867 .M87 2019");
    mainDoc.addField("isbn_display", "9781119221258 (hardback)");
    mainDoc.addField("isbn_display", "1119221250");
    mainDoc.addField("oclc_id_display", "1082972111");

    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);
    assertEquals("Murphy, Brian G., 1966-"
        + " <b>Veterinary oral and maxillofacial pathology.</b>"
        + " Wiley-Blackwell, 2020.",
        docs.get(0).getFieldValue("cite_preescaped_display"));
    assertEquals(Arrays.asList("9781119221258 (hardback)","1119221250"),
        docs.get(0).getFieldValues("isbn_display"));
    assertEquals("1082972111",docs.get(0).getFieldValue("oclc_id_display"));

  }

  @Test
  public void bibCallNumberForClosedStacksHoldingsWithNonLcHoldingCallNum()
      throws SQLException, IOException, XMLStreamException {

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 1449673);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "1449673");
    mainDoc.addField("lc_callnum_full", "Z340 .P58");
    mainDoc.addField("oclc_id_display", "63966981");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);

    assertEquals(3,docs.size());
    assertEquals("Z340 .P58",docs.get(0).getFieldValue("callnum_display"));
    assertEquals("Dante Z340 .P58",docs.get(1).getFieldValue("callnum_display"));
    assertEquals("Film 7087 reel 198 no.11",docs.get(2).getFieldValue("callnum_display"));

    Set<String> callNumbers = CallNumberBrowse.collateCallNumberList(docs);
    assertEquals( 3, callNumbers.size() );

    assertEquals("63966981",docs.get(0).getFieldValue("oclc_id_display"));
  }

  @Test
  public void letterOnlyCallNumber()
      throws SQLException, IOException, XMLStreamException {

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 11438152);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "11438152");
    mainDoc.addField("lc_callnum_full", "Q");
    mainDoc.addField("lc_bib_display", "Q");
    mainDoc.addField("url_access_json", "");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);

    assertEquals(1, docs.size());
    assertEquals("Q",docs.get(0).getFieldValue("callnum_display"));
    assertEquals(true,docs.get(0).getFieldValue("lc_b"));

    Set<String> callNumbers = CallNumberBrowse.collateCallNumberList(docs);
    assertTrue( callNumbers.isEmpty() );
    assertEquals("Q - Science > Q - Science (General)",docs.get(0).getFieldValue("classification_display"));
  }

  @Test
  public void shelfHoldingsCallLcBibCall() throws SQLException, IOException, XMLStreamException {

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 7259947);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, holdings.get(mfhdId).active);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "7259947");
    mainDoc.addField("lc_callnum_full", "D16.3 .B65");
    mainDoc.addField("lc_bib_display", "D16.3 .B65");
    mainDoc.addField("lc_callnum_full", "1-2-m.178");
    mainDoc.addField("lc_callnum_full", "Archives 1-2-m.178");
    mainDoc.addField("lc_callnum_full", "D16.3 .B72");

    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(inventory, mainDoc, holdings);

    for (SolrInputDocument doc : docs)
      System.out.println(ClientUtils.toXML(doc));

    /*
<doc boost="1.0"><field name="bibid">7259947</field>
 <field name="cite_preescaped_display"></field>
 <field name="id">7259947.1</field>
 <field name="callnum_sort">Archives 1-2-m.178 0 7259947.1</field>
 <field name="callnum_display">Archives 1-2-m.178</field>
 <field name="lc_b">false</field>
 <field name="availability_json">
    {"available":true,"availAt":{"Kroch Library Rare &amp; Manuscripts (Non-Circulating)":""}}</field>
 <field name="location">Kroch Library Rare &amp; Manuscripts</field>
 <field name="location">Kroch Library Rare &amp; Manuscripts &gt; Main Collection</field>
 <field name="online">At the Library</field></doc>
<doc boost="1.0">
 <field name="bibid">7259947</field>
 <field name="cite_preescaped_display"></field>
 <field name="id">7259947.2</field>
 <field name="callnum_sort">D16.3 .B72 0 7259947.2</field>
 <field name="callnum_display">D16.3 .B72</field>
 <field name="lc_b">true</field>
 <field name="availability_json">{"available":true,"availAt":{"Olin Library":""}}</field>
 <field name="classification_display">D - World History &gt; D - History (General) &gt; D1-24.5 - General</field>
 <field name="location">Olin Library</field>
 <field name="location">Olin Library &gt; Main Collection</field>
 <field name="online">At the Library</field></doc>
<doc boost="1.0">
 <field name="bibid">7259947</field>
 <field name="cite_preescaped_display">
 </field><field name="id">7259947.3</field>
 <field name="callnum_sort">D16.3 .B65 0 7259947.3</field>
 <field name="callnum_display">D16.3 .B65</field>
 <field name="lc_b">true</field>
 <field name="availability_json">
    {"available":true,"availAt":{"Kroch Library Rare &amp; Manuscripts (Non-Circulating)":""}}</field>
 <field name="classification_display">D - World History &gt; D - History (General) &gt; D1-24.5 - General</field>
 <field name="location">Kroch Library Rare &amp; Manuscripts</field>
 <field name="location">Kroch Library Rare &amp; Manuscripts &gt; Main Collection</field>
 <field name="online">At the Library</field></doc>

     */

  }

}
