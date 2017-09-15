package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.voyager.TestUtil.convertStreamToString;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.voyager.Holdings.Holding;

public class HoldingsTest {

  String expectedMarc1184953 =
  "000    00214nx  a2200097z  4500\n"+
  "001    1184953\n"+
  "004    969430\n"+
  "008    0005172u    8   4001uu   0000000\n"+
  "014 1  ‡a AED2310CU001\n"+
  "014 0  ‡9 001182083\n"+
  "852 00 ‡b ilr,anx ‡h HC59.7 ‡i .B16 1977 ‡x os=y\n";

  static Connection voyagerTest = null;
  static Connection voyagerLive = null;
  static Map<String,Holding> examples;

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
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("holdings_examples.json")){
      ObjectMapper mapper = new ObjectMapper();
      examples = mapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$" , ""),
          new TypeReference<HashMap<String,Holding>>() {});
    }
  }

  @Test
  public void getHoldingByHoldingId() throws SQLException, IOException, XMLStreamException {
    Holding holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1184953);
    assertEquals(examples.get("expectedJson1184953").toJson(),holding.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holding.date)).toString());
    assertEquals(expectedMarc1184953,holding.record.toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    assertEquals(examples.get("expectedJson9850688").toJson(),holding.toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holding.date)).toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 2202712);
    assertEquals(examples.get("expectedJson2202712").toJson(),holding.toJson());
    assertEquals("Tue Jul 14 13:03:11 EDT 2009",(new Date(1000L*holding.date)).toString());

    assertEquals(examples.get("expectedJson1131911").toJson(),
        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911).toJson());
    assertEquals(examples.get("expectedJson413836").toJson(),
        Holdings.retrieveHoldingsByHoldingId(voyagerTest, 413836).toJson());
  }

  @Test
  public void getHoldingByBibId() throws SQLException, IOException, XMLStreamException {
    List<Holding> holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 969430);
    assertEquals(2,holdings.size());
    assertEquals(examples.get("expectedJson1184953").toJson(),holdings.get(0).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(0).date)).toString());
    assertEquals(expectedMarc1184953,holdings.get(0).record.toString());
    assertEquals(examples.get("expectedJson1184954").toJson(),holdings.get(1).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(1).date)).toString());

    holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 9520154);
    assertEquals(1,holdings.size());
    assertEquals(examples.get("expectedJson9850688").toJson(),holdings.get(0).toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holdings.get(0).date)).toString());
  }

  @Test
  public void roundTripHoldingThroughJson() throws SQLException, IOException, XMLStreamException {
    Holding h1 = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    String j1 = h1.toJson();
    Holding h2 = Holdings.extractHoldingFromJson(j1);
    String j2 = h2.toJson();
    assertEquals(examples.get("expectedJson9850688").toJson(),j2);
  }
}
