package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.xml.stream.XMLStreamException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Items.ItemList;
import edu.cornell.library.integration.voyager.VoyagerDBConnection;

public class BibliographicSummaryTest {

  static VoyagerDBConnection testDB = null;
  static Connection voyagerTest = null;
  static Connection voyagerLive = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {

    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");
    voyagerTest = testDB.connection;
//  voyagerLive = VoyagerDBConnection.getLiveConnection("database.properties");

  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }


  @Test
  public void partiallyInTempLocAndUnavail() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4546769);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals(
        "{\"available\":true,"
        +"\"availAt\":{\"ILR Library (Ives Hall)\":\"HD9710.A1 W38\"},\"unavailAt\":{}}",
        b.toJson());
  }

  @Test
  public void partialReserveHoldings() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2026746);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals(
        "{\"available\":true,\"availAt\":{\"Africana Library (Africana Center)\":\"Video 9\","
        + "\"Library Annex\":\"Video 1192\"}}",b.toJson());
  }

  @Test
  public void online() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 10023626);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals("{\"online\":true}",b.toJson());
  }

  @Test
  public void onReserve() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 9628566);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals("{\"available\":true,\"availAt\":{\"ILR Library Reserve\":\"HD31.2 .G95 2017\"}}",
        b.toJson());
  }

  @Test
  public void multivolMixedAvail() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals("{\"available\":true,"
        +         "\"availAt\":{\"Library Annex\":\"Q1 .N282\"}}",b.toJson());
  }

  @Test
  public void missing() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1055);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals("{\"available\":false,"
        +         "\"unavailAt\":{\"ILR Library (Ives Hall)\":\"JS39 .M95 Oversize\"}}",b.toJson());
  }

  @Test
  public void electricSheep() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4442869);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId, h.get(mfhdId).active);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    assertEquals(
    "{\"available\":true,"
    +"\"availAt\":{\"Library Annex\":\"PS3554.I3 D6x 1996\","
    +             "\"Olin Library\":\"PS3554.I3 D6 1996\","
    +             "\"Kroch Library Rare & Manuscripts (Non-Circulating)\""
    +                                    ":\"Rare Books PS3554.I3 D6 1996\","
    +             "\"ILR Library (Ives Hall)\":\"PS3554.I3 D6 1996\","
    +             "\"Mann Library\":\"PS3554.I3 D6 1996\"}}",b.toJson());
  }

}
