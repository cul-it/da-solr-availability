package edu.cornell.library.integration.folio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.naming.AuthenticationException;

public class MonitorFolioChanges {

  private static final String CURRENT_TO_KEY = "avail";

  public static void main(String[] args) throws IOException, SQLException, InterruptedException, AuthenticationException {

    Map<String, String> env = System.getenv();
    String configFile = env.get("configFile");
    if (configFile == null)
      throw new IllegalArgumentException("configFile must be set in environment to valid file path.");
    Properties prop = new Properties();
    File f = new File(configFile);
    if (f.exists()) {
      try ( InputStream is = new FileInputStream(f) ) { prop.load( is ); }
    } else System.out.println("File does not exist: "+configFile);


    try (
        Connection inventory = DriverManager.getConnection(
            prop.getProperty("databaseURLCurrent"),prop.getProperty("databaseUserCurrent"),prop.getProperty("databasePassCurrent"));
        PreparedStatement queueAvail = inventory.prepareStatement
            ("INSERT INTO availQueue ( hrid, priority, cause, record_date ) VALUES (?,?,?,?)");
        PreparedStatement queueGen = inventory.prepareStatement
            ("INSERT INTO generationQueue ( hrid, priority, cause, record_date ) VALUES (?,?,?,?)");
        PreparedStatement getTitle = inventory.prepareStatement
            ("SELECT title FROM bibRecsSolr WHERE bib_id = ?");
        PreparedStatement trimUserChangeLog = inventory.prepareStatement
            ("DELETE FROM userChanges WHERE date < SUBDATE( NOW(), INTERVAL 3 MINUTE )");
        PreparedStatement getUserChangeTotals = inventory.prepareStatement
            ("SELECT id, COUNT(*) FROM userChanges GROUP BY 1")) {

      OkapiClient okapi = new OkapiClient( prop, "Folio" );

      Timestamp time = Change.getCurrentToDate( inventory, CURRENT_TO_KEY );
      if (time == null) {
        time = new Timestamp(Calendar.getInstance().getTime().getTime()-(10*60*60*1000));
        System.out.println("No starting timestamp in DB, defaulting to 10 hours ago.");
      }
      System.out.println(time);

      while ( true ) {
        Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-10_000);//now minus 10 seconds
        final Timestamp since = time;

        trimUserChangeLog.executeUpdate();
        queueForIndex( ChangeDetector.detectChangedInstances( inventory, okapi, since ),
            queueGen, getTitle, getUserChangeTotals );
        Map<String, Set<Change>> changedBibs =
            ChangeDetector.detectChangedHoldings( inventory, okapi, since );
        queueForIndex(changedBibs,queueGen,getTitle, getUserChangeTotals);
        queueForIndex(changedBibs,queueAvail,getTitle, getUserChangeTotals);
        queueForIndex(ChangeDetector.detectChangedItems( inventory, okapi, since ),
            queueAvail, getTitle, getUserChangeTotals );
        queueForIndex( ChangeDetector.detectChangedLoans( inventory, okapi, since ),
            queueAvail, getTitle, getUserChangeTotals );
        queueForIndex( ChangeDetector.detectChangedRequests( inventory, okapi, since ),
            queueAvail, getTitle, getUserChangeTotals );
        queueForIndex( ChangeDetector.detectChangedOrderLines( inventory, okapi, since ),
            queueAvail, getTitle, getUserChangeTotals );
        queueForIndex( ChangeDetector.detectChangedOrders( inventory, okapi, since ),
            queueAvail, getTitle, getUserChangeTotals );

        Thread.sleep(12_000); //12 seconds
        time = newTime;
        Change.setCurrentToDate( time, inventory, CURRENT_TO_KEY );
      }
    }
  }

  public static Map<Integer,Set<Change>> eliminateCarryovers( 
      Map<Integer,Set<Change>> newChanges, Map<Integer,Set<Change>> oldChanges) {
    if ( oldChanges.isEmpty() )
      return newChanges;
    List<Integer> bibsToRemove = new ArrayList<>();
    for (Integer newBibId : newChanges.keySet()) {
      if ( ! oldChanges.containsKey(newBibId) )
        continue;
      List<Change> changesToRemove = new ArrayList<>();
      for ( Change c : newChanges.get(newBibId) )
        if (oldChanges.get(newBibId).contains(c))
          changesToRemove.add(c);
      newChanges.get(newBibId).removeAll(changesToRemove);
      if (newChanges.get(newBibId).isEmpty())
        bibsToRemove.add(newBibId);
    }
    for (Integer i : bibsToRemove)
      newChanges.remove(i);
    return newChanges;
  }


  private static void queueForIndex( Map<String, Set<Change>> changedBibs,
      PreparedStatement q, PreparedStatement getTitle, PreparedStatement getUserTotals)
          throws SQLException {

    if ( changedBibs.isEmpty() ) return;

    Map<String,Integer> userCounts = new HashMap<>();
    try ( ResultSet rs = getUserTotals.executeQuery() ) {
      while ( rs.next() ) userCounts.put(rs.getString(1), rs.getInt(2));
    }

    int i = 0;
    for (Entry<String,Set<Change>> e : changedBibs.entrySet()) {
      int bibId = Integer.valueOf(e.getKey());
      getTitle.setInt(1,bibId);
      String title = null;
      try (ResultSet rs1 = getTitle.executeQuery() ) {while (rs1.next()) title = rs1.getString(1);}
      String causes = e.getValue().toString();
      System.out.println(bibId+" ("+title+") "+causes);
      int priority = 6;
      for ( Change c : e.getValue() ) {
        if ( c.userId != null && userCounts.containsKey(c.userId) ) {
          int changeCount = userCounts.get(c.userId);
          if ( changeCount < 25 )
            priority = 1;
          else if (changeCount < 100 )
            priority = 4;
          else if (changeCount < 500)
            priority = 5;
        }
      }
      if (causes.length() > 65_000 ) {
        causes = causes.substring(0, 65_000);
        System.out.printf("Causes for bib %d truncated to 65k characters.\n",bibId);
      }
      q.setInt(1, bibId);
      q.setInt(2,priority);
      q.setString(3, causes);
      q.setTimestamp(4,getMinChangeDate( e.getValue() ));
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
