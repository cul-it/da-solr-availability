package edu.cornell.library.integration.folio;

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class MonitorFolioChanges {

  private static final String CURRENT_TO_KEY = "avail";

  public static void main(String[] args) throws IOException, SQLException, InterruptedException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }

    try (
        Connection inventory = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement queueAvail = inventory.prepareStatement
            ("INSERT INTO availabilityQueue ( hrid, priority, cause, record_date ) VALUES (?,?,?,?)");
        PreparedStatement queueGen = inventory.prepareStatement
            ("INSERT INTO generationQueue ( hrid, priority, cause, record_date ) VALUES (?,?,?,?)");
        PreparedStatement getTitle = inventory.prepareStatement
            ("SELECT title FROM bibRecsSolr WHERE bib_id = ?")) {

      OkapiClient okapi = new OkapiClient(
          prop.getProperty("okapiUrlFolio"),
          prop.getProperty("okapiTokenFolio"),
          prop.getProperty("okapiTenantFolio"));

      Timestamp time = Change.getCurrentToDate( inventory, CURRENT_TO_KEY );
      if (time == null) {
        time = new Timestamp(Calendar.getInstance().getTime().getTime()-(10*60*60*1000));
        System.out.println("No starting timestamp in DB, defaulting to 10 hours ago.");
      }
      System.out.println(time);

//          new ItemStatuses(), new Items(), new OpenOrder(), new RecentIssues()));
      while ( true ) {
        Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-6000);
        final Timestamp since = time;

        queueForIndex(
            ChangeDetector.detectChangedInstances( inventory, okapi, since ),queueGen, getTitle );
        Map<String, Set<Change>> changedBibs =
            ChangeDetector.detectChangedHoldings( inventory, okapi, since );
        queueForIndex(changedBibs,queueGen,getTitle);
        queueForIndex(changedBibs,queueAvail,getTitle);
//        queueForIndex(
//            ChangeDetector.detectChangedItems( inventory, okapi, since ), queueAvail, getTitle );
//        detectors.parallelStream().map(c -> detectChanges(c, okapi, since))
//        .flatMap(m -> m.entrySet().stream())
//        .collect(Collectors.toMap(Entry::getKey,Entry::getValue,(v1,v2) -> { v1.addAll(v2); return v1; }));

        Thread.sleep(500);
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


  private static void queueForIndex(
      Map<String, Set<Change>> changedBibs, PreparedStatement q, PreparedStatement getTitle)
          throws SQLException {

    int i = 0;
    for (Entry<String,Set<Change>> e : changedBibs.entrySet()) {
      int bibId = Integer.valueOf(e.getKey());
      getTitle.setInt(1,bibId);
      String title = null;
      try (ResultSet rs1 = getTitle.executeQuery() ) {while (rs1.next()) title = rs1.getString(1);}
      String causes = e.getValue().toString();
      System.out.println(bibId+" ("+title+") "+causes);
      if (causes.length() > 65_000 ) {
        causes = causes.substring(0, 65_000);
        System.out.printf("Causes for bib %d truncated to 65k characters.\n",bibId);
      }
      q.setInt(1, bibId);
      q.setInt(2,isBatch( e.getValue() )?6:1);
      q.setString(3, causes);
      q.setTimestamp(4,getMinChangeDate( e.getValue() ));
      q.addBatch();
      if (++i == 100) { q.executeBatch(); i=0; }
    }
    q.executeBatch();
  }

  private static boolean isBatch(Set<Change> changes) {
    for (Change c : changes) if (c.type.equals(Change.Type.ITEM_BATCH)) return true;
    return false;
  }

  private static Timestamp getMinChangeDate(Set<Change> changes) {
    Timestamp d = new Timestamp(System.currentTimeMillis());
    for (Change c : changes)
      if ( d.after(c.date()) )
        d = c.date();
    return d;
  }

}
