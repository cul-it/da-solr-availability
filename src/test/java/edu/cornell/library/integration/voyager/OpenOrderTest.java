package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.ItemList;
import edu.cornell.library.integration.voyager.OpenOrder;

public class OpenOrderTest {

  static Connection voyagerTest = null;
//  static Connection voyagerLive = null;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException, IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("org.sqlite.JDBC");
    voyagerTest = DriverManager.getConnection("jdbc:sqlite:src/test/resources/voyagerTest.db");
//    Class.forName("oracle.jdbc.driver.OracleDriver");
//    voyagerLive = DriverManager.getConnection(
//        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
  }


  @Test
  public void onOrder() throws SQLException {
    OpenOrder o = new OpenOrder(voyagerTest, 10797688);
    assertEquals( "On order as of 4/17/19", o.notes.get(11093964) );
  }

  @Test
  public void orderPreprocessing() throws SQLException {

    // Pending order, show pre-order processing, and line item copy status date
    OpenOrder o = new OpenOrder(voyagerTest, 10797795);
    assertEquals( "In pre-order processing as of 4/17/19", o.notes.get(11094047) );

    // Pending order, show pre-order processing, no available line item copy status date
    o = new OpenOrder(voyagerTest, 10797341);
    assertEquals( "In pre-order processing", o.notes.get(11093619) );

  }

  @Test
  public void availableForPurchaseWithCancelledOrder() throws SQLException, IOException, XMLStreamException {

    int bibId = 9386182;

    // First, we can find the order information independently
    OpenOrder o = new OpenOrder(voyagerTest, bibId);
    assertEquals( "Order cancelled", o.notes.get(9720246) );

    // Second, we don't pull it into holdings because of the call number
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, bibId);
    ItemList items = Items.retrieveItemsForHoldings(voyagerTest, null, bibId, holdings);
    holdings.summarizeItemAvailability(items);
    holdings.applyOpenOrderInformation(voyagerTest, bibId);
    assertEquals( "Available for the Library to Purchase", holdings.get(9720246).call );
    assertNull( holdings.get(9720246).orderNote );

    // But without that call number, the lack of items will allow the order note
    holdings.get(9720246).call = "ABC123";
    holdings.applyOpenOrderInformation(voyagerTest, bibId);
    assertEquals( "Order cancelled", holdings.get(9720246).orderNote );

  }

  @Test
  public void oneBibTwoMfhdsTwoOrderedCopies() throws SQLException, IOException, XMLStreamException {

    int bibId = 10705932;

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, bibId);
    ItemList items = Items.retrieveItemsForHoldings(voyagerTest, null, bibId, holdings);
    holdings.summarizeItemAvailability(items);
    holdings.applyOpenOrderInformation(voyagerTest, bibId);

    // Uris holdings shows order note
    assertEquals( "On order as of 3/4/19" , holdings.get(11002767).orderNote );

    // Law holdings suppresses note due to item present on mfhd
    assertNull( holdings.get(11123231).orderNote );


  }

  @Test
  public void oneBibOneOrderedCopySecondMfhd() throws SQLException, IOException, XMLStreamException {

    int bibId = 10663989;

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, bibId);
    ItemList items = Items.retrieveItemsForHoldings(voyagerTest, null, bibId, holdings);
    holdings.summarizeItemAvailability(items);
    holdings.applyOpenOrderInformation(voyagerTest, bibId);

    assertEquals( "On order as of 2/19/19, On order as of 2/22/19" , holdings.get(10961068).orderNote );
    assertEquals( "On order" , holdings.get(10975017).orderNote );
  }

  @Test
  public void oneBibTwoOrderedCopiesOnOneMfhd() throws SQLException, IOException, XMLStreamException {

    int bibId = 10825496;

    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, bibId);
    ItemList items = Items.retrieveItemsForHoldings(voyagerTest, null, bibId, holdings);
    holdings.summarizeItemAvailability(items);
    holdings.applyOpenOrderInformation(voyagerTest, bibId);

    assertEquals( "2 copies ordered as of 5/6/19" , holdings.get(11121177).orderNote );
    assertEquals( "On order" ,       holdings.get(11121178).orderNote );

  }
}
