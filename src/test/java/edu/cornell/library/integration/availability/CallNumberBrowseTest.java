package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class CallNumberBrowseTest {

  static Connection voyagerTest = null;
//  static Connection voyagerLive = null;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException {
    Class.forName("org.sqlite.JDBC");
    voyagerTest = DriverManager.getConnection("jdbc:sqlite:src/test/resources/voyagerTest.db");

/*    // Connect to live Voyager database
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
    prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    voyagerLive = DriverManager.getConnection(
      prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
*/
  }

  @Test
  public void multipleCopies() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 4442869);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "4442869");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(mainDoc, holdings);

    assertEquals(3,docs.size());
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">4442869</field>"
        + "<field name=\"id\">4442869.1</field>"
        + "<field name=\"callnum_sort\">Rare Books PS3554.I3 D6 1996 0 4442869.1</field>"
        + "<field name=\"callnum_display\">Rare Books PS3554.I3 D6 1996</field>"
        + "<field name=\"availability_json\">{\"available\":true,"
        +    "\"availAt\":{\"Kroch Library Rare &amp; Manuscripts (Non-Circulating)\":\"Rare Books PS3554.I3 D6 1996\"}}</field>"
        + "<field name=\"location\">Kroch Library Rare &amp; Manuscripts</field>"
        + "<field name=\"location\">Kroch Library Rare &amp; Manuscripts &gt; Main Collection</field>"
        + "<field name=\"online\">At the Library</field>"
        + "<field name=\"shelfloc\">true</field></doc>",
        ClientUtils.toXML(docs.get(0)));
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">4442869</field>"
        + "<field name=\"id\">4442869.2</field>"
        + "<field name=\"callnum_sort\">PS3554.I3 D6 1996 0 4442869.2</field>"
        + "<field name=\"callnum_display\">PS3554.I3 D6 1996</field>"
        + "<field name=\"availability_json\">{\"available\":true,"
        +    "\"availAt\":{\"Olin Library\":\"PS3554.I3 D6 1996\","
        +                 "\"ILR Library (Ives Hall)\":\"PS3554.I3 D6 1996\","
        +                 "\"Mann Library\":\"PS3554.I3 D6 1996\"}}</field>"
        + "<field name=\"location\">Olin Library</field>"
        + "<field name=\"location\">Olin Library &gt; Main Collection</field>"
        + "<field name=\"location\">ILR Library</field>"
        + "<field name=\"location\">ILR Library &gt; Main Collection</field>"
        + "<field name=\"location\">Mann Library</field>"
        + "<field name=\"location\">Mann Library &gt; Main Collection</field>"
        + "<field name=\"online\">At the Library</field>"
        + "<field name=\"shelfloc\">true</field></doc>",
        ClientUtils.toXML(docs.get(1)));
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">4442869</field>"
        + "<field name=\"id\">4442869.3</field>"
        + "<field name=\"callnum_sort\">PS3554.I3 D6x 1996 0 4442869.3</field>"
        + "<field name=\"callnum_display\">PS3554.I3 D6x 1996</field>"
        + "<field name=\"availability_json\">{\"available\":true,"
        +     "\"availAt\":{\"Library Annex\":\"PS3554.I3 D6x 1996\"},"
        +     "\"unavailAt\":{\"Olin Library\":\"PS3554.I3 D6x 1996\"}}</field>"
        + "<field name=\"location\">Olin Library</field>"
        + "<field name=\"location\">Olin Library &gt; Main Collection</field>"
        + "<field name=\"location\">Library Annex</field>"
        + "<field name=\"online\">At the Library</field>"
        + "<field name=\"shelfloc\">true</field></doc>",
        ClientUtils.toXML(docs.get(2)));

  }

  @Test
  public void serial() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
    for (int mfhdId : holdings.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      holdings.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }

    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "329763");
    mainDoc.addField("lc_callnum_full", "Q1 .N2");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(mainDoc, holdings);

//    for (SolrInputDocument doc : docs) System.out.println(ClientUtils.toXML(doc));

    assertEquals(2,docs.size());
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">329763</field>"
        + "<field name=\"id\">329763.1</field>"
        + "<field name=\"callnum_sort\">Q1 .N282 0 329763.1</field>"
        + "<field name=\"callnum_display\">Q1 .N282</field>"
        + "<field name=\"availability_json\">{\"available\":true,\"availAt\":{\"Library Annex\":\"Q1 .N282\"}}</field>"
        + "<field name=\"location\">Library Annex</field>"
        + "<field name=\"online\">At the Library</field>"
        + "<field name=\"shelfloc\">true</field></doc>",
        ClientUtils.toXML(docs.get(0)));
    assertEquals(
        "<doc boost=\"1.0\">"
        + "<field name=\"bibid\">329763</field>"
        + "<field name=\"id\">329763.2</field>"
        + "<field name=\"callnum_sort\">Q1 .N2 0 329763.2</field>"
        + "<field name=\"callnum_display\">Q1 .N2</field>"
        + "<field name=\"availability_json\">{\"available\":true,\"availAt\":{\"Veterinary Library (Schurman Hall)\":null}}</field>"
        + "<field name=\"location\">Veterinary Library</field>"
        + "<field name=\"location\">Veterinary Library &gt; Main Collection</field>"
        + "<field name=\"online\">At the Library</field>"
        + "<field name=\"flag\">Bibliographic Call Number</field></doc>",
        ClientUtils.toXML(docs.get(1)));

  }

}