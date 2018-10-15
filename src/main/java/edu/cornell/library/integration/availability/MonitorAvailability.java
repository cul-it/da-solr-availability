package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.RecentIssues;

public class MonitorAvailability {

  private static final String CURRENT_TO_KEY = "avail";

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException, InterruptedException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Class.forName("com.mysql.jdbc.Driver");

    try (Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement queueIndex = inventoryDB.prepareStatement
            ("INSERT INTO availabilityQueue ( bib_id, priority, cause, record_date ) VALUES (?,1,?,?)");
        PreparedStatement getTitle = inventoryDB.prepareStatement
            ("SELECT title FROM bibRecsSolr WHERE bib_id = ?")) {

      Timestamp time = RecordsToSolr.getCurrentToDate( inventoryDB, CURRENT_TO_KEY );
      if (time == null) {
        time = new Timestamp(Calendar.getInstance().getTime().getTime()-(10*60*60*1000));
        System.out.println("No starting timestamp in DB, defaulting to 10 hours ago.");
      }
      System.out.println(time);

      Map<Integer,Set<Change>> carryoverChanges = new HashMap<>();
      while ( true ) {
        Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-6000);
        Map<Integer,Set<Change>> changedBibs =
            Items.detectChangedItemStatuses(voyager, time, new HashMap<Integer,Set<Change>>());
        Items.detectItemReserveStatusChanges(voyager, time, changedBibs);
        RecentIssues.detectNewReceiptBibs(voyager, time, changedBibs);
        Map<Integer,Set<Change>> newlyChangedBibs = RecordsToSolr.eliminateCarryovers(
            RecordsToSolr.duplicateMap(changedBibs), carryoverChanges);
        carryoverChanges = changedBibs;
        if ( newlyChangedBibs.size() > 0 )
          queueForIndex( newlyChangedBibs, queueIndex, getTitle );
        Thread.sleep(500);
        time = newTime;
        RecordsToSolr.setCurrentToDate( time, inventoryDB, CURRENT_TO_KEY );
      }
    }
  }

  private static void queueForIndex(
      Map<Integer, Set<Change>> changedBibs, PreparedStatement q, PreparedStatement getTitle)
          throws SQLException {

    int i = 0;
    for (Entry<Integer,Set<Change>> e : changedBibs.entrySet()) {
      int bibId = e.getKey();
      getTitle.setInt(1,bibId);
      String title = null;
      try (ResultSet rs1 = getTitle.executeQuery() ) {while (rs1.next()) title = rs1.getString(1);}
      String causes = e.getValue().toString();
      System.out.println(bibId+" ("+title+") "+causes);
      q.setInt(1, bibId);
      q.setString(2, causes);
      q.setTimestamp(3,getMinChangeDate( e.getValue() ));
      q.addBatch();
      if (++i == 100) { q.executeBatch(); i=0; }
    }
    q.executeBatch();
  }

  private static Timestamp getMinChangeDate(Set<Change> changes) {
    Timestamp d = new Timestamp(System.currentTimeMillis());
    for (Change c : changes)
      if ( d.after(c.date()) )
        d = c.date();
    return d;
  }

}
