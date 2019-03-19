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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.RecentIssues;

public class DetectChangesWithoutModDates {

  private final static String insertAvailQ =
      "INSERT INTO availabilityQueue (bib_id, priority, cause, record_date) VALUES (?,6,?,NOW())";
  
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Class.forName("com.mysql.jdbc.Driver");

    try (Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"))) {


      go(voyager,inventoryDB);
    }
  }

  private static void go ( Connection voyager, Connection inventory )
      throws SQLException, IOException {

    processRecentIssueChanges( voyager, inventory );
    processDueDateChanges( voyager, inventory );
  }

  private static void processRecentIssueChanges(Connection voyager, Connection inventory)
      throws SQLException, JsonProcessingException {

    // Load previous issues from inventory
    Map<Integer,String> prevValues = new HashMap<>();
    try ( Statement stmt = inventory.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT bib_id, json FROM recentIssues")) {
      while (rs.next())
      prevValues.put(rs.getInt(1),rs.getString(2));
    }
    System.out.println("Previous bibs with recent issues: "+prevValues.size());

    // Get changes in current Voyager Data
    Map<Integer,Set<Change>> changes = RecentIssues.detectAllChangedBibs(voyager, prevValues, new HashMap<>());
    System.out.println("Changes: "+changes.size());
    if ( changes.isEmpty())
      return;

    // Push changes to Solr
    try ( PreparedStatement p = inventory.prepareStatement(insertAvailQ) ) {
      for (Entry<Integer,Set<Change>> e : changes.entrySet()) {
        p.setInt(1, e.getKey());
        p.setString(2, e.getValue().toString());
        p.addBatch();
        System.out.println(e.toString());
      }
      p.executeBatch();
    }
  }

  private static void processDueDateChanges(Connection voyager, Connection inventory)
      throws SQLException, JsonProcessingException {

    // Load previous due dates from inventory
    Map<Integer,String> prevValues = new HashMap<>();
    try ( Statement stmt = inventory.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT bib_id, json FROM itemDueDates")) {
      while (rs.next())
      prevValues.put(rs.getInt(1),rs.getString(2));
    }
    System.out.println("Previous bibs with checked out items: "+prevValues.size());

    // Get changes in current Voyager Data
    Map<Integer,Set<Change>> changes = Items.detectChangedItemDueDates(voyager, prevValues, new HashMap<>());
    System.out.println("Changes: "+changes.size());
    if ( changes.isEmpty())
      return;

    // Push changes to Solr
    try ( PreparedStatement p = inventory.prepareStatement(insertAvailQ) ) {
      int i = 0;
      for (Entry<Integer,Set<Change>> e : changes.entrySet()) {
        p.setInt(1, e.getKey());
        p.setString(2, e.getValue().toString());
        p.addBatch();
        if ( ++i % 1000 == 0 ) { 
          p.executeBatch();
        }
        System.out.println(e.toString());
      }
      p.executeBatch();
    }

  }

}
