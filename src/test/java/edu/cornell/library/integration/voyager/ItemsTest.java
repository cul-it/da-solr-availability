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
    String expected =
        "{\"id\":2282772,"
        + "\"mfhd_id\":1234567,"
        + "\"barcode\":\"31924067383830\","
        + "\"copy_number\":1,"
        + "\"sequence_number\":1,"
        + "\"year\":null,"
        + "\"chron\":null,"
        + "\"enum\":null,"
        + "\"caption\":null,"
        + "\"holds\":0,"
        + "\"recalls\":0,"
        + "\"on_reserve\":false,"
        + "\"location\":{\"code\":\"rmc,anx\",\"number\":203,\"name\":\"Kroch Library Rare & Manuscripts"
        +         " (Request in advance)\",\"library\":\"Kroch Library Rare & Manuscripts\"},"
        + "\"type\":{\"id\":9,\"name\":\"nocirc\"},"
        + "\"status\":{\"available\":true,\"codes\":{\"1\":\"Not Charged\"},\"due\":null,\"date\":null},"
        + "\"date\":959745600}";
    for (Item item : items) {
      assertEquals(expected,item.toJson());
      assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*item.date)).toString());
      assertNull(item.status.date);
    }
  }

  @Test
  public void getItemsByHoldingId2() throws SQLException, JsonProcessingException {
    List<Item> items = Items.retrieveItemsByHoldingId(voyager, 1184953);
    String expected =
        "{\"id\":2236014,"
        + "\"mfhd_id\":1184953,"
        + "\"barcode\":\"31924002209538\","
        + "\"copy_number\":1,"
        + "\"sequence_number\":1,"
        + "\"year\":null,"
        + "\"chron\":null,"
        + "\"enum\":null,"
        + "\"caption\":null,"
        + "\"holds\":0,"
        + "\"recalls\":0,"
        + "\"on_reserve\":false,"
        + "\"location\":{\"code\":\"ilr,anx\",\"number\":52,\"name\":\"Library Annex\",\"library\":\"Library Annex\"},"
        + "\"type\":{\"id\":3,\"name\":\"book\"},"
        + "\"status\":{\"available\":true,"
        +             "\"codes\":{\"1\":\"Not Charged\"},"
        +             "\"due\":null,"
        +             "\"date\":1456742278},"
        + "\"date\":959745600}";
    for (Item item : items) {
      assertEquals(expected,item.toJson());
      assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*item.date)).toString());
      assertEquals("Mon Feb 29 05:37:58 EST 2016",(new Date(1000L*item.status.date)).toString());
    }
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
