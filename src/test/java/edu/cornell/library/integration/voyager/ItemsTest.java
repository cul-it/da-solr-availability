package edu.cornell.library.integration.voyager;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.voyager.Items.Item;

public class ItemsTest {

  String expectedJson2282772 =
      "{\"id\":2282772,"
      + "\"mfhdId\":1234567,"
      + "\"copyNumber\":1,"
      + "\"sequenceNumber\":1,"
      + "\"enum\":null,"
      + "\"caption\":null,"
      + "\"holds\":0,"
      + "\"recalls\":0,"
      + "\"onReserve\":false,"
      + "\"location\":{\"code\":\"rmc,anx\","
      +               "\"number\":203,"
      +               "\"name\":\"Kroch Library Rare & Manuscripts (Request in advance)\","
      +               "\"library\":\"Kroch Library Rare & Manuscripts\"},"
      + "\"type\":{\"id\":9,\"name\":\"nocirc\"},"
      + "\"status\":{\"available\":true,"
      +             "\"codes\":{\"1\":\"Not Charged\"},"
      +             "\"due\":null,"
      +             "\"date\":null},"
      + "\"date\":959745600}";

  String expectedJson2236014 =
      "{\"id\":2236014,"
      + "\"mfhdId\":1184953,"
      + "\"copyNumber\":1,"
      + "\"sequenceNumber\":1,"
      + "\"enum\":null,"
      + "\"caption\":null,"
      + "\"holds\":0,"
      + "\"recalls\":0,"
      + "\"onReserve\":false,"
      + "\"location\":{\"code\":\"ilr,anx\","
      +               "\"number\":52,"
      +               "\"name\":\"Library Annex\","
      +               "\"library\":\"Library Annex\"},"
      + "\"type\":{\"id\":3,\"name\":\"book\"},"
      + "\"status\":{\"available\":true,"
      +             "\"codes\":{\"1\":\"Not Charged\"},"
      +             "\"due\":null,"
      +             "\"date\":1456742278},"
      + "\"date\":959745600}";

  static Connection voyager = null;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException, IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
  }

  @Test
  public void getItemsByHoldingId1() throws SQLException, JsonProcessingException {
    List<Item> items = Items.retrieveItemsByHoldingId(voyager, 1234567);
    assertEquals(1,items.size());
    assertEquals(expectedJson2282772,items.get(0).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*items.get(0).date)).toString());
    assertNull(items.get(0).status.date);
  }

  @Test
  public void getItemsByHoldingId2() throws SQLException, JsonProcessingException {
    List<Item> items = Items.retrieveItemsByHoldingId(voyager, 1184953);
    assertEquals(1,items.size());
    assertEquals(expectedJson2236014,items.get(0).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*items.get(0).date)).toString());
    assertEquals("Mon Feb 29 05:37:58 EST 2016",(new Date(1000L*items.get(0).status.date)).toString());
//     System.out.println(item.toJson().replaceAll("\"", "\\\\\""));
  }

  @Test
  public void getItemByItemIdTest() throws SQLException, JsonProcessingException {
    Item item = Items.retrieveItemByItemId(voyager, 2236014);
    List<Item> items = Items.retrieveItemsByHoldingId(voyager, 1184953);
    assertEquals( items.get(0).toJson(), item.toJson());
  }

  @Test
  public void roundTripItemsThroughJsonTest() throws SQLException, IOException {
    Item item1 = Items.retrieveItemByItemId(voyager, 2236014);
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
