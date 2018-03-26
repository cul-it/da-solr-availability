package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class BibliographicSummaryTest {

  static Connection voyagerTest = null;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException {

    // Connect to test Voyager database
    Class.forName("org.sqlite.JDBC");
    voyagerTest = DriverManager.getConnection("jdbc:sqlite:src/test/resources/voyagerTest.db");

  }

  @Test
  public void partiallyInTempLocAndUnavail() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4546769);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println(b.toJson());
  }

  @Test
  public void partialReserveHoldings() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 2026746);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println(b.toJson());
  }

  @Test
  public void online() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 10023626);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println("Online: "+b.toJson());
  }

  @Test
  public void onReserve() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 9628566);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println(b.toJson());
  }

  @Test
  public void multivolMixedAvail() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 329763);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println(b.toJson());
  }

  @Test
  public void missing() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1055);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println(b.toJson());
  }

  @Test
  public void electricSheep() throws SQLException, IOException, XMLStreamException {
    HoldingSet h = Holdings.retrieveHoldingsByBibId(voyagerTest, 4442869);
    for (int mfhdId : h.getMfhdIds()) {
      ItemList i = Items.retrieveItemsByHoldingId(voyagerTest, mfhdId);
      h.get(mfhdId).summarizeItemAvailability(i.getItems().get(mfhdId));
    }
    BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(h);
    System.out.println(b.toJson());
  }

}
