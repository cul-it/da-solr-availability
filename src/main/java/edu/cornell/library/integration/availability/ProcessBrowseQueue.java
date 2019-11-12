package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ProcessBrowseQueue {

  public static void main(String[] args) throws IOException, SQLException, InterruptedException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(System.getenv("CONFIG_FILE"))){
      prop.load(in);
    }

    try (
        Connection inventory = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        Connection headings = DriverManager.getConnection(
            prop.getProperty("headingsDBUrl"),prop.getProperty("headingsDBUser"),prop.getProperty("headingsDBPass"));
        Statement stmt = inventory.createStatement();
        PreparedStatement nextInQueue = inventory.prepareStatement(
            "SELECT heading_id FROM browseQueue ORDER BY priority LIMIT 1");
        PreparedStatement queueItemsForHead = inventory.prepareStatement("SELECT id FROM browseQueue WHERE heading_id = ?");
        PreparedStatement deprioritize = inventory.prepareStatement("UPDATE browseQueue SET priority = 9 WHERE id = ?");
        PreparedStatement dequeue = inventory.prepareStatement("DELETE FROM browseQueue WHERE id = ?");
        ){

      for (int i = 0; i < 1; i++){

        Set<Integer> ids = new HashSet<>();
        Integer headingId = null;

        stmt.execute("LOCK TABLES browseQueue WRITE");
        try (  ResultSet rs = nextInQueue.executeQuery() ) { while ( rs.next() ) { headingId = rs.getInt("heading_id"); } }

        if ( headingId == null ) { stmt.execute("UNLOCK TABLES"); Thread.sleep(3000); continue; }

        queueItemsForHead.setInt(1, headingId);
        try( ResultSet rs = queueItemsForHead.executeQuery() ) { while ( rs.next() ) ids.add( rs.getInt(1) ); }

        for ( int id : ids ) { deprioritize.setInt(1, id); deprioritize.addBatch(); }
        deprioritize.executeBatch();

        stmt.execute("UNLOCK TABLES");

        processHeading( headings, headingId );
      }
    }
  }

  private static PreparedStatement headingsCounts = null;
  private static void processHeading(Connection headings, Integer headingId) throws SQLException {

    if ( headingsCounts == null )
      headingsCounts = headings.prepareStatement("SELECT heading_category, count(*) FROM bib2heading WHERE heading_id = ?");

    Map<Integer,Integer> categoryCounts = new HashMap<>();
    headingsCounts.setInt(1, headingId);
    try ( ResultSet rs = headingsCounts.executeQuery() ) {
      while ( rs.next() )
        categoryCounts.put(rs.getInt(1), rs.getInt(2));
    }
  }

}
