package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.voyager.Holdings.Holding;

public class HoldingsTest {

  String expectedJson1184953 =
  "{\"id\":1184953,"
  + "\"location\":{\"code\":\"ilr,anx\","
  +               "\"number\":52,"
  +               "\"name\":\"Library Annex\","
  +               "\"library\":\"Library Annex\"},"
  + "\"date\":959745600}";

  String expectedJson1184954 =
  "{\"id\":1184954,"
  + "\"copyNum\":\"2\","
  + "\"location\":{\"code\":\"ilr\","
  +               "\"number\":51,"
  +               "\"name\":\"ILR Library (Ives Hall)\","
  +               "\"library\":\"ILR Library\"},"
  + "\"date\":959745600}";

  String expectedJson9850688 =
  "{\"id\":9850688,"
  + "\"desc\":[\"no.177-182 (2016)\"],"
  + "\"location\":{\"code\":\"was\","
  +               "\"number\":139,"
  +               "\"name\":\"Kroch Library Asia\","
  +               "\"library\":\"Kroch Library Asia\"},"
  + "\"date\":1495138879}";

  String expectedJson2202712 =
  "{\"id\":2202712,"
  +"\"notes\":[\"1987 bound with: Quest no.91-95, sasa BR 128 B8 E53+ no.93.  --1989 bound with: Quest no.105, sasa BR 128 B8 E53+ no.105-106.\"],"
  +"\"desc\":[\"1978, 1980-1982, 1985, 1987-1992, 1994-1996\"],"
  +"\"location\":{\"code\":\"sasa\",\"number\":122,\"name\":\"Kroch Library Asia\",\"library\":\"Kroch Library Asia\"},"
  +"\"date\":1247590991,"
  +"\"boundWith\":{\"3910960\":{\"masterItemId\":3910960,"
  +                            "\"masterBibId\":2095674,"
  +                            "\"masterTitle\":\"Accelerated Mahaweli, Sri Lanka Development Programme /\","
  +                            "\"masterEnum\":\"no.105-106\","
  +                            "\"thisEnum\":\"1989\","
  +                            "\"status\":{\"available\":true,\"codes\":{\"1\":\"Not Charged\"}}},"
  +               "\"3131680\":{\"masterItemId\":3131680,"
  +                            "\"masterBibId\":1575369,"
  +                            "\"masterTitle\":\"Emerging community : two dialoguing religions of South-East Asia /\","
  +                            "\"masterEnum\":\"no.91-95\","
  +                            "\"thisEnum\":\"1987\","
  +                            "\"status\":{\"available\":true,\"codes\":{\"1\":\"Not Charged\"}}}}}";

  String expectedJson1131911 =
  "{\"id\":1131911,"
  +"\"desc\":[\"v.1-2;\"],"
  +"\"supplDesc\":[\"Fifth suppl.:1982-1988\"],"
  +"\"location\":{\"code\":\"law\","
  +              "\"number\":63,"
  +              "\"name\":\"Law Library (Myron Taylor Hall)\","
  +              "\"library\":\"Law Library\"},"
  +"\"date\":959745600}";

  String expectedJson413836 =
  "{\"id\":413836,"
  +"\"desc\":[\"1959-1974\"],"
  +"\"indexDesc\":[\"1959/1974\"],"
  +"\"location\":{\"code\":\"olin,anx\",\"number\":101,\"name\":\"Library Annex\",\"library\":\"Library Annex\"},"
  +"\"date\":959745600}";

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
  public void getHoldingByHoldingId() throws SQLException, IOException, XMLStreamException {
    Holding holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1184953);
    assertEquals(expectedJson1184953,holding.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holding.date)).toString());
    assertEquals(expectedMarc1184953,holding.record.toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    assertEquals(expectedJson9850688,holding.toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holding.date)).toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 2202712);
    assertEquals(expectedJson2202712,holding.toJson());
    assertEquals("Tue Jul 14 13:03:11 EDT 2009",(new Date(1000L*holding.date)).toString());

    assertEquals(expectedJson1131911,Holdings.retrieveHoldingsByHoldingId(voyagerTest, 1131911).toJson());
    assertEquals(expectedJson413836,Holdings.retrieveHoldingsByHoldingId(voyagerTest, 413836).toJson());
  }

  @Test
  public void getHoldingByBibId() throws SQLException, IOException, XMLStreamException {
    List<Holding> holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 969430);
    assertEquals(2,holdings.size());
    assertEquals(expectedJson1184953,holdings.get(0).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(0).date)).toString());
    assertEquals(expectedMarc1184953,holdings.get(0).record.toString());
    assertEquals(expectedJson1184954,holdings.get(1).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(1).date)).toString());

    holdings = Holdings.retrieveHoldingsByBibId(voyagerTest, 9520154);
    assertEquals(1,holdings.size());
    assertEquals(expectedJson9850688,holdings.get(0).toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holdings.get(0).date)).toString());
  }

  @Test
  public void roundTripHoldingThroughJson() throws SQLException, IOException, XMLStreamException {
    Holding h1 = Holdings.retrieveHoldingsByHoldingId(voyagerTest, 9850688);
    String j1 = h1.toJson();
    Holding h2 = Holdings.extractHoldingFromJson(j1);
    String j2 = h2.toJson();
    assertEquals(expectedJson9850688,j2);
  }
}
