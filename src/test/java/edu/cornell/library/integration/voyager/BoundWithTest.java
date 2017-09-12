package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.marc.DataField;

public class BoundWithTest {

  String expectedBoundWithJson4102195 =
  "{\"masterItemId\":4102195,"
  +"\"masterBibId\":2248567,"
  +"\"masterTitle\":\"Black biographical dictionaries, 1790-1950\","
  +"\"masterEnum\":\"title# 291 fiche 3/5\","
  +"\"thisEnum\":\"\","
  +"\"barcode\":\"31924064096195\","
  +"\"status\":{\"available\":true,"
  +            "\"codes\":{\"1\":\"Not Charged\"},"
  +            "\"due\":null,"
  +            "\"date\":null}}";

  String expectedBoundWithJson1726636 =
  "{\"masterItemId\":1726636,"
  +"\"masterBibId\":3827392,"
  +"\"masterTitle\":\"Monograph of the Palaeontographical Society\","
  +"\"masterEnum\":\"v.14\","
  +"\"thisEnum\":\"v.14\","
  +"\"barcode\":\"31924004546812\","
  +"\"status\":{\"available\":true,"
  +            "\"codes\":{\"1\":\"Not Charged\"},"
  +            "\"due\":null,"
  +            "\"date\":null}}";

  static Connection voyager = null;
  static Connection inventory = null;

  @BeforeClass
  public static void connect() throws SQLException, ClassNotFoundException, IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
    inventory = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
  }

  @Test
  public void boundWithMainConstructor() throws SQLException, JsonProcessingException {
    DataField f = new DataField(6,"876",' ',' ',"‡p 31924064096195");
    BoundWith b = BoundWith.from876Field(voyager, inventory, f);
    assertEquals(expectedBoundWithJson4102195,b.toJson());
    f = new DataField(6,"876",' ',' ',"‡3 v.14 ‡p 31924004546812");
    b = BoundWith.from876Field(voyager, inventory, f);
    assertEquals(expectedBoundWithJson1726636,b.toJson());
//    System.out.println(b.toJson().replaceAll("\"", "\\\\\""));
  }
}
