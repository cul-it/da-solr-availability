package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
    Class.forName("oracle.jdbc.driver.OracleDriver");
    voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
  }

  @Test
  public void detectNewReceiptsTest() throws SQLException {
    Map<Integer,List<String>> issues = RecentIssues.detectNewReceipts(voyagerLive, Timestamp.valueOf("2018-05-04 12:00:00.0"));
    for (Entry<Integer, List<String>> e : issues.entrySet()) {
      System.out.println(e.getKey());
      System.out.println("  "+String.join("\n  ",e.getValue()));
    }
  }

//  @Test
  public void getRecentIssuesForHoldingTest() throws SQLException {
    List<String> issues = RecentIssues.getByHoldingId(voyagerLive, 449213);
    System.out.println(String.join("\n", issues));
  }

//  @Test
  public void getRecentIssuesForBibTest() throws SQLException {
//    Map<Integer,List<String>> issues = RecentIssues.getByBibId(voyagerLive, 369282);
    Map<Integer,List<String>> issues = RecentIssues.getByBibId(voyagerLive, 115983);
    for (Entry<Integer, List<String>> e : issues.entrySet()) {
      System.out.println(e.getKey());
      System.out.println("  "+String.join("\n  ",e.getValue()));
    }
  }

}
