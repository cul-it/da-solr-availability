package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.voyager.TestUtil.convertStreamToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class ItemsTest {

  static Connection voyagerTest = null;
  static Connection voyagerLive = null;
  static Map<String,ItemList> examples ;

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
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("items_examples.json")){
      ObjectMapper mapper = new ObjectMapper();
      examples = mapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$" , ""),
          new TypeReference<HashMap<String,ItemList>>() {});
    }

  }

  @Test
  public void getItemsByHoldingId() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1234567);
    assertEquals(examples.get("expectedJson2282772").toJson(),items.toJson());
    assertEquals(1,items.mfhdCount());
    assertEquals(1,items.itemCount());

    items = Items.retrieveItemsByHoldingId(voyagerTest, 1184953);
    assertEquals(examples.get("expectedJson2236014").toJson(),items.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L* items.getItem(1184953,2236014).date )).toString());

    items = Items.retrieveItemsByHoldingId(voyagerTest, 9975971);
    assertEquals(examples.get("expectedJson10013120").toJson(),items.toJson());
    assertFalse( items.getItem(9975971,10013120).status.available );

    items = Items.retrieveItemsByHoldingId(voyagerTest, 2202712);
    assertEquals(examples.get("expectedJson2202712").toJson(),items.toJson());
  }

  @Test
  public void getItemsWithMultipleHoldingsTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingIds(voyagerTest, Arrays.asList(1234567));
    assertEquals(examples.get("expectedJson2282772").toJson(),items.toJson());

    items = Items.retrieveItemsByHoldingIds(voyagerTest, Arrays.asList(
        4977210,4977214,5860317,7367226,7371275,7371277,7371279,7371281,7371283,7371284,
        7371302,7383225,7383631,7383632,7383752,7383755,7383965,7383966,7387210,7391702  ));
    assertEquals(examples.get("expectedJsonELECTRICSHEEP").toJson(),items.toJson());
  }

  @Test
  public void onHoldTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 2932);
    assertEquals(examples.get("expectedJson18847").toJson(),items.toJson());
  }

  @Test
  public void recalledTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 6511093);
    assertEquals(examples.get("expectedJSON8060353").toJson(),items.toJson());
  }

  @Test
  public void onReserveTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingIds(voyagerTest, Arrays.asList(10287643,9957560));
    assertEquals(examples.get("expectedJsonOnReserve").toJson(),items.toJson());
  }

  @Test
  public void multiVolTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingIds(voyagerTest, Arrays.asList(4521000));
    assertEquals(examples.get("expectedJsonMultiVol").toJson(),items.toJson());
  }

  @Test
  public void missingTest() throws SQLException, JsonProcessingException {
    ItemList items = Items.retrieveItemsByHoldingId(voyagerTest, 1055);
    assertEquals(examples.get("expectedJsonMissing").toJson(),items.toJson());
//    System.out.println(items.toJson());
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
}
