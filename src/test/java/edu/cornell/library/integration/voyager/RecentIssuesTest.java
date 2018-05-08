package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

public class RecentIssuesTest {
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
  public void getRecentIssuesForHoldingTest() throws SQLException {
    List<String> issues = RecentIssues.getByHoldingId(voyagerTest, 449213);
    assertEquals(25,issues.size());
    assertEquals("sayi 793 (2018 Ocak)",issues.get(0));
    assertEquals("sayi 769 (2016 Ocak.)",issues.get(24));
  }

  @Test
  public void getRecentIssuesForBibTest() throws SQLException {
    Map<Integer,List<String>> issues = RecentIssues.getByBibId(voyagerTest, 369282);
    assertEquals(1,issues.size());
    assertTrue(issues.containsKey(449213));
    assertEquals(25,issues.get(449213).size());
    assertEquals("sayi 793 (2018 Ocak)",issues.get(449213).get(0));
    assertEquals("sayi 778 (2016 Ekim)",issues.get(449213).get(15));
  }

}
