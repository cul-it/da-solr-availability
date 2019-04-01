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
  static Connection voyagerLive = null;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException, IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("org.sqlite.JDBC");
    voyagerTest = DriverManager.getConnection("jdbc:sqlite:src/test/resources/voyagerTest.db");
    Class.forName("oracle.jdbc.driver.OracleDriver");
    voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
  }

  @Test
  public void onOrderTest() throws SQLException, IOException, XMLStreamException {

    int bibId = 9386182;

    // First, we can find the order information independently
    OpenOrder o = new OpenOrder(voyagerLive, bibId);
    assertEquals( "Order cancelled", o.note );

    // Second, we don't pull it into holdings because of the call number
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerLive, bibId);
    ItemList items = Items.retrieveItemsForHoldings(voyagerLive, null, bibId, holdings);
    holdings.summarizeItemAvailability(items);
    holdings.applyOpenOrderInformation(voyagerLive, bibId);
    assertEquals( "Available for the Library to Purchase", holdings.get(9720246).call );
    assertNull( holdings.get(9720246).orderNote );

    // But without that call number, the lack of items will allow the order note
    holdings.get(9720246).call = "ABC123";
    holdings.applyOpenOrderInformation(voyagerLive, bibId);
    assertEquals( "Order cancelled", holdings.get(9720246).orderNote );
  }

}
