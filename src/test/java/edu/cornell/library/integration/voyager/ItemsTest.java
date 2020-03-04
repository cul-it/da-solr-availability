package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.voyager.TestUtil.convertStreamToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class ItemsTest {

  static VoyagerDBConnection testDB = null;
  static Connection voyagerTest = null;
  static Connection voyagerLive = null;
  static Map<String,ItemList> examples ;

  @BeforeClass
  public static void connect() throws SQLException, IOException {

    testDB = new VoyagerDBConnection("src/test/resources/voyagerTest.sql");
    voyagerTest = testDB.connection;
//  voyagerLive = VoyagerDBConnection.getLiveConnection("database.properties");

    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("items_examples.json")){
      ObjectMapper mapper = new ObjectMapper();
      examples = mapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$" , ""),
          new TypeReference<HashMap<String,ItemList>>() {});
    }
  }

  @AfterClass
  public static void cleanUp() throws SQLException {
    testDB.close();
  }

  @Test
  public void getItemsByHoldingId() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1234567, true);
    assertEquals(examples.get("expectedJson2282772").toJson(),items.toJson());
    assertEquals(1,items.mfhdCount());
    assertEquals(1,items.itemCount());

    items = Items.retrieveItemsByHoldingId(voyagerTest, 1184953, true);
    assertEquals(examples.get("expectedJson2236014").toJson(),items.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L* items.getItem(1184953,2236014).date )).toString());

    items = Items.retrieveItemsByHoldingId(voyagerTest, 9975971, true);
    assertEquals(examples.get("expectedJson10013120").toJson(),items.toJson());
    assertFalse( items.getItem(9975971,10013120).status.available );

    items = Items.retrieveItemsByHoldingId(voyagerTest, 2202712, true);
    assertEquals(examples.get("expectedJson2202712").toJson(),items.toJson());
  }

  @Test
  public void onHoldTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 2932, true);
    assertEquals(examples.get("expectedJson18847").toJson(),items.toJson());
  }

  @Test
  public void recalledTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 6511093, true);
    assertEquals(examples.get("expectedJSON8060353").toJson(),items.toJson());
  }

  @Test
  public void onReserveTest() throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest,9628566);
    ItemList items = Items.retrieveItemsForHoldings(voyagerTest,null,9628566,holdings);
    assertEquals(examples.get("expectedJsonOnReserve").toJson(),items.toJson());
  }

  @Test
  public void multiVolTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 4521000, true);
    assertEquals(examples.get("expectedJsonMultiVol").toJson(),items.toJson());
  }

  @Test
  public void missingTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1055, true);
    assertEquals(examples.get("expectedJsonMissing").toJson(),items.toJson());
  }

  @Test
  public void lost() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 5404964, true);
    assertEquals(examples.get("expectedLost").toJson(),items.toJson());
  }

  @Test
  public void checkedOutReserve() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1144752, true);
//    System.out.println(items.toJson());
    assertEquals(examples.get("expectedCheckedOutReserve").toJson(),items.toJson());
  }

  @Test
  public void emptyItem() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1016218, true);
    assertEquals(examples.get("expectedBib833840").toJson(),items.toJson());
  }

  @Test
  public void getItemByItemIdTest() throws SQLException, JsonProcessingException {
    Item item = Items.retrieveItemByItemId(voyagerTest,2236014);
    item.mfhdId = null; // mfhdId is not present in example mode, so remove for comparison
    assertEquals( examples.get("expectedJson2236014").getItem(1184953,2236014).toJson(),
        item.toJson());
    }

  @Test
  public void roundTripItemsThroughJsonTest() throws SQLException, IOException {
    Item item1 = Items.retrieveItemByItemId(voyagerTest, 2236014);
    String json1 = item1.toJson();
    Item item2 = Items.extractItemFromJson(json1);
    String json2 = item2.toJson();

    assertEquals(json1,json2);
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*item1.date)).toString());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*item2.date)).toString());
    assertEquals("Mon Feb 29 05:37:58 EST 2016",(new Date(1000L*item1.status.date)).toString());
    assertEquals("Mon Feb 29 05:37:58 EST 2016",(new Date(1000L*item2.status.date)).toString());
  }

  @Test
  public void partiallySuppressedHoldings() throws SQLException, IOException, XMLStreamException {
    {
      HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest,1449673);
      ItemList items = Items.retrieveItemsForHoldings(voyagerTest,null,1449673,holdings);
      assertEquals(examples.get(
      "RMC and Annex copies active, not other Annex").toJson(),items.toJson());
    }
    {
      HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest,11764);
      ItemList items = Items.retrieveItemsForHoldings(voyagerTest,null,11764,holdings);
      assertEquals(examples.get("Annex copy active, not Mann").toJson(),items.toJson());
    }
    {
      HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest,34985);
      ItemList items = Items.retrieveItemsForHoldings(voyagerTest,null,34985,holdings);
      assertEquals(examples.get(
      "Annex copy, rare Annex copy active, not Microfilm").toJson(),items.toJson());
    }
    {
      HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest,5487364);
      ItemList items = Items.retrieveItemsForHoldings(voyagerTest,null,5487364,holdings);
      assertEquals(examples.get("Olin copy active, not RMC copy").toJson(),items.toJson());
    }
    {
      HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyagerTest,301608);
      ItemList items = Items.retrieveItemsForHoldings(voyagerTest,null,301608,holdings);
      assertEquals(examples.get(
      "RMC copy, Annex copy & 1st microfilm active, 2nd microfilm inactive").toJson(),items.toJson());
    }
  }

}
