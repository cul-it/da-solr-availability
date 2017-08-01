package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
        "{\"item_id\":2282772,"
        + "\"mfhd_id\":1234567,"
        + "\"item_barcode\":\"31924067383830\","
        + "\"copy_number\":1,"
        + "\"item_sequence_number\":1,"
        + "\"year\":null,"
        + "\"chron\":null,"
        + "\"item_enum\":null,"
        + "\"caption\":null,"
        + "\"holds_placed\":0,"
        + "\"recalls_placed\":0,"
        + "\"on_reserve\":\"N\","
        + "\"location\":{\"code\":\"rmc,anx\",\"number\":203,\"name\":\"Kroch Library Rare & Manuscripts"
        +         " (Request in advance)\",\"library\":\"Kroch Library Rare & Manuscripts\"},"
        + "\"type\":{\"id\":9,\"name\":\"nocirc\"},"
        + "\"status\":{\"available\":true,\"codes\":{\"1\":\"Not Charged\"},\"current_due_date\":null}}";
    for (Item item : items)
      assertEquals(expected,item.toJson());
  }

  @Test
  public void getItemsByHoldingId2() throws SQLException, JsonProcessingException {
    List<Item> items = Items.retrieveItemsByHoldingId(voyager, 1184953);
    String expected =
        "{\"item_id\":2236014,"
        + "\"mfhd_id\":1184953,"
        + "\"item_barcode\":\"31924002209538\","
        + "\"copy_number\":1,"
        + "\"item_sequence_number\":1,"
        + "\"year\":null,"
        + "\"chron\":null,"
        + "\"item_enum\":null,"
        + "\"caption\":null,"
        + "\"holds_placed\":0,"
        + "\"recalls_placed\":0,"
        + "\"on_reserve\":\"N\","
        + "\"location\":{\"code\":\"ilr,anx\",\"number\":52,\"name\":\"Library Annex\",\"library\":\"Library Annex\"},"
        + "\"type\":{\"id\":3,\"name\":\"book\"},"
        + "\"status\":{\"available\":true,\"codes\":{\"1\":\"Not Charged\"},\"current_due_date\":null}}";
    for (Item item : items)
      assertEquals(expected,item.toJson());
//     System.out.println(item.toJson().replaceAll("\"", "\\\\\""));
  }

  @Test
  public void getItemByItemIdTest() throws SQLException, JsonProcessingException {
    Item item = Items.retrieveItemByItemId(voyager, 2236014);
    List<Item> items = Items.retrieveItemsByHoldingId(voyager, 1184953);
    assertEquals( items.get(0).toJson(), item.toJson());
  }
}
