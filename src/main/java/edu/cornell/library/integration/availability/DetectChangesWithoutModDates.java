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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.voyager.ItemStatuses;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.RecentIssues;

public class DetectChangesWithoutModDates {

  private final static String insertAvailQ =
      "INSERT INTO availabilityQueue (bib_id, priority, cause, record_date) VALUES (?,6,?,NOW())";
  
  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }

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
    processRequestChanges( voyager, inventory );
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
    Map<Integer,Set<Change>> changes = detectChangedItemDueDates(voyager, prevValues, new HashMap<>());
    System.out.println("Changes: "+changes.size());
    if ( changes.isEmpty())
      return;

    // Push changes to Solr
    addChangesToAvailabilityQueue( inventory, changes );

  }

  private static void processRequestChanges(Connection voyager, Connection inventory)
      throws SQLException {

    // Load previous requests from inventory
    Map<Integer,String> prevValues = new HashMap<>();
    try ( Statement stmt = inventory.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT bib_id, json FROM itemRequests")) {
      while (rs.next())
      prevValues.put(rs.getInt(1),rs.getString(2));
    }
    System.out.println("Previous bibs with requests: "+prevValues.size());

    // Get changes in current Voyager Data
    Map<Integer,Set<Change>> changes = detectRequests(voyager, prevValues, new HashMap<>());
    System.out.println("Changes: "+changes.size());
    if ( changes.isEmpty())
      return;

    // Push changes to Solr
    addChangesToAvailabilityQueue( inventory, changes );

  }

  private static Map<Integer,Set<Change>> detectRequests(
      Connection voyager, Map<Integer,String> prevValues, Map<Integer,Set<Change>> changes )
          throws SQLException{
    Map<Integer,String> allRequests = ItemStatuses.collectAllRequests(voyager);
    for (Integer bibId : prevValues.keySet()) {
      if (allRequests.containsKey(bibId)) {
        if (! prevValues.get(bibId).equals(allRequests.get(bibId))) {
          Set<Change> t = new HashSet<>();
          t.add(new Change(Change.Type.CIRC,null,
              "Request Modified "+prevValues.get(bibId)+" ==> "+allRequests.get(bibId), null, null));
          changes.put(bibId, t);
        }
        allRequests.remove(bibId);
      } else {
        Set<Change> t = new HashSet<>();
        t.add(new Change(Change.Type.CIRC,null,"Request Disappeared "+prevValues.get(bibId), null, null));
        changes.put(bibId, t);
      }
    }
    for (Integer bibId : allRequests.keySet()) {
      Set<Change> t = new HashSet<>();
      t.add(new Change(Change.Type.CIRC,null,"Request Appeared "+allRequests.get(bibId),null,null));
      changes.put(bibId,t);
    }
    return changes;
  }

  private static Map<Integer,Set<Change>> detectChangedItemDueDates(
      Connection voyager, Map<Integer,String> prevValues, Map<Integer,Set<Change>> changes )
          throws SQLException, JsonProcessingException {
    Map<Integer,String> allDueDates = Items.collectAllCurrentDueDates( voyager );
    for (Integer bibId : prevValues.keySet()) {
      if (allDueDates.containsKey(bibId)) {
        if (! prevValues.get(bibId).equals(allDueDates.get(bibId))) {
          Set<Change> t = new HashSet<>();
          t.add(new Change(Change.Type.CIRC,null,
              "Due Date Modified "+prevValues.get(bibId)+" ==> "+allDueDates.get(bibId), null, null));
          changes.put(bibId, t);
        }
        allDueDates.remove(bibId);
      } else {
        Set<Change> t = new HashSet<>();
        t.add(new Change(Change.Type.CIRC,null,"Due Date Disappeared "+prevValues.get(bibId), null, null));
        changes.put(bibId, t);
      }
    }
    for (Integer bibId : allDueDates.keySet()) {
      Set<Change> t = new HashSet<>();
      t.add(new Change(Change.Type.CIRC,null,"Due Date Appeared "+allDueDates.get(bibId),null,null));
      changes.put(bibId,t);
    }
    return changes;
  }

  private static void addChangesToAvailabilityQueue( Connection inventory, Map<Integer,Set<Change>> changes )
      throws SQLException {
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
