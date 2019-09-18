package edu.cornell.library.integration.changes;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.cornell.library.integration.voyager.ItemStatuses;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.OpenOrder;
import edu.cornell.library.integration.voyager.RecentIssues;
import edu.cornell.library.integration.voyager.ReserveModule;

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

    ComboPooledDataSource cpds = new ComboPooledDataSource();
    try { cpds.setDriverClass("oracle.jdbc.driver.OracleDriver"); }
    catch (PropertyVetoException e) { e.printStackTrace(); System.exit(1); }
    cpds.setJdbcUrl(prop.getProperty("voyagerDBUrl"));
    cpds.setUser(prop.getProperty("voyagerDBUser"));
    cpds.setPassword(prop.getProperty("voyagerDBPass"));
    cpds.setMinPoolSize(5);
    cpds.setInitialPoolSize(5);
    try (
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement queueIndex = inventoryDB.prepareStatement
            ("INSERT INTO availabilityQueue ( bib_id, priority, cause, record_date ) VALUES (?,1,?,?)");
        PreparedStatement getTitle = inventoryDB.prepareStatement
            ("SELECT title FROM bibRecsSolr WHERE bib_id = ?")) {

      Timestamp time = Change.getCurrentToDate( inventoryDB, CURRENT_TO_KEY );
      if (time == null) {
        time = new Timestamp(Calendar.getInstance().getTime().getTime()-(10*60*60*1000));
        System.out.println("No starting timestamp in DB, defaulting to 10 hours ago.");
      }
      System.out.println(time);

      Set<ChangeDetector> detectors = new HashSet<>(Arrays.asList(
          new ItemStatuses(), new Items(), new ReserveModule(), new OpenOrder(), new RecentIssues()));
      Map<Integer,Set<Change>> carryoverChanges = new HashMap<>();
      while ( true ) {
        Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-6000);
        final Timestamp since = time;

        Map<Integer,Set<Change>> changedBibs =
        detectors.parallelStream()
        .map(c -> detectChanges(c, cpds, since))
        .flatMap(m -> m.entrySet().stream())
        .collect(Collectors.toMap(Entry::getKey,Entry::getValue,(v1,v2) -> { v1.addAll(v2); return v1; }));

        Map<Integer,Set<Change>> newlyChangedBibs = eliminateCarryovers(duplicateMap(changedBibs), carryoverChanges);
        carryoverChanges = changedBibs;
        if ( newlyChangedBibs.size() > 0 )
          queueForIndex( newlyChangedBibs, queueIndex, getTitle );
        Thread.sleep(500);
        time = newTime;
        Change.setCurrentToDate( time, inventoryDB, CURRENT_TO_KEY );
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

  private static Map<Integer,Set<Change>> duplicateMap( Map<Integer,Set<Change>> m1 ) {
    Map<Integer,Set<Change>> m2 = new HashMap<>();
    for (Entry<Integer,Set<Change>> e : m1.entrySet())
      m2.put(e.getKey(), new HashSet<>(e.getValue()));
    return m2;
  }


  private static Map<Integer,Set<Change>> detectChanges (
      ChangeDetector detector, ComboPooledDataSource voypool, Timestamp since ) {
    try ( Connection voyager = voypool.getConnection() ) {
      return detector.detectChanges(voyager, since);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
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
