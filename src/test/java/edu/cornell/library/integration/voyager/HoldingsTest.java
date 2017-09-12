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
  + "\"bib_id\":969430,"
  + "\"copy_num\":null,"
  + "\"notes\":[],"
  + "\"desc\":[],"
  + "\"suppl_desc\":[],"
  + "\"index_desc\":[],"
  + "\"location\":{\"code\":\"ilr,anx\","
  +               "\"number\":52,"
  +               "\"name\":\"Library Annex\","
  +               "\"library\":\"Library Annex\"},"
  + "\"date\":959745600}";

  String expectedJson1184954 =
  "{\"id\":1184954,"
  + "\"bib_id\":969430,"
  + "\"copy_num\":\"2\","
  + "\"notes\":[],"
  + "\"desc\":[],"
  + "\"suppl_desc\":[],"
  + "\"index_desc\":[],"
  + "\"location\":{\"code\":\"ilr\","
  +               "\"number\":51,"
  +               "\"name\":\"ILR Library (Ives Hall)\","
  +               "\"library\":\"ILR Library\"},"
  + "\"date\":959745600}";

  String expectedJson9850688 =
  "{\"id\":9850688,"
  + "\"bib_id\":9520154,"
  + "\"copy_num\":null,"
  + "\"notes\":[],"
  + "\"desc\":[\"no.177-182 (2016)\"],"
  + "\"suppl_desc\":[],"
  + "\"index_desc\":[],"
  + "\"location\":{\"code\":\"was\","
  +               "\"number\":139,"
  +               "\"name\":\"Kroch Library Asia\","
  +               "\"library\":\"Kroch Library Asia\"},"
  + "\"date\":1495138879}";

  String expectedMarc1184953 =
  "000    00214nx  a2200097z  4500\n"+
  "001    1184953\n"+
  "004    969430\n"+
  "008    0005172u    8   4001uu   0000000\n"+
  "014 1  ‡a AED2310CU001\n"+
  "014 0  ‡9 001182083\n"+
  "852 00 ‡b ilr,anx ‡h HC59.7 ‡i .B16 1977 ‡x os=y\n";

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
  public void getHoldingByHoldingId() throws SQLException, IOException, XMLStreamException {
    Holding holding = Holdings.retrieveHoldingsByHoldingId(voyager, 1184953);
    assertEquals(expectedJson1184953,holding.toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holding.date)).toString());
    assertEquals(expectedMarc1184953,holding.record.toString());

    holding = Holdings.retrieveHoldingsByHoldingId(voyager, 9850688);
    assertEquals(expectedJson9850688,holding.toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holding.date)).toString());
  }

  @Test
  public void getHoldingByBibId() throws SQLException, IOException, XMLStreamException {
    List<Holding> holdings = Holdings.retrieveHoldingsByBibId(voyager, 969430);
    assertEquals(2,holdings.size());
    assertEquals(expectedJson1184953,holdings.get(0).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(0).date)).toString());
    assertEquals(expectedMarc1184953,holdings.get(0).record.toString());
    assertEquals(expectedJson1184954,holdings.get(1).toJson());
    assertEquals("Wed May 31 00:00:00 EDT 2000",(new Date(1000L*holdings.get(1).date)).toString());

    holdings = Holdings.retrieveHoldingsByBibId(voyager, 9520154);
    assertEquals(1,holdings.size());
    assertEquals(expectedJson9850688,holdings.get(0).toJson());
    assertEquals("Thu May 18 16:21:19 EDT 2017",(new Date(1000L*holdings.get(0).date)).toString());
//    System.out.println(holdings.get(1).toJson().replaceAll("\"","\\\\\""));
//    System.out.println((new Date(1000L*holdings.get(0).date)).toString());
//    System.out.println(holdings.get(1).record.toString());
  }
}
