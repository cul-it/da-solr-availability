package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Items;

public class MonitorRecordChanges {

  private static final String CURRENT_TO_KEY = "record";

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException, XMLStreamException, SolrServerException, InterruptedException {
    Timestamp since = Timestamp.valueOf("2018-05-14 10:00:00.0");

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
    Class.forName("com.mysql.jdbc.Driver");
    Connection inventoryDB = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));

    since = RecordsToSolr.getCurrentToDate( since, inventoryDB, CURRENT_TO_KEY );

    Timestamp time = since;
    System.out.println(time);
    Map<Integer,Set<Change>> carryoverChangeBlocks = new HashMap<>();
    Map<Integer,Set<Change>> carryoverExpectedChanges = new HashMap<>();
    while ( true ) {
      Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-120_000);

      Map<Integer,Set<Change>> changedBibs = 
          Items.detectChangedItems(voyager, time, new HashMap<Integer,Set<Change>>() );
      Holdings.detectChangedHoldings(voyager, time, changedBibs);
      BibliographicSummary.detectChangedBibs(voyager, time, changedBibs);

      addCarriedoverExpectedChangesToFoundChanges(changedBibs, carryoverExpectedChanges);

      Map<Integer,Set<Change>> newCarryoverExpectations = new HashMap<>();
      Map<Integer,Set<Change>> newCarryoverBlocks = new HashMap<>();
      Map<Integer,Set<Change>> solrUpdates = new HashMap<>();
      for (Entry<Integer,Set<Change>> e : changedBibs.entrySet()) {
        Integer bibId = e.getKey();
        for (Change c : e.getValue()) {
          boolean blocked = false;
          boolean metNotBlocked = false;
          if (carryoverChangeBlocks.containsKey(bibId) &&
              carryoverChangeBlocks.get(bibId).contains(c))
            blocked = true;
          else if (changeIsMet(inventoryDB,c))
            metNotBlocked = true;
          if (metNotBlocked)
            addChangeToSet(solrUpdates,bibId,c);
          if (metNotBlocked || blocked)
            addChangeToSet(newCarryoverBlocks,bibId,c);
          else {
            addChangeToSet(newCarryoverExpectations,bibId,c);
            if (Duration.between(c.changeDate.toInstant(), Instant.now()).getSeconds() > 3600)
              System.out.println("Long-term carry over "+bibId+": "+c);
          }
        }
      }
      carryoverExpectedChanges = newCarryoverExpectations;
      carryoverChangeBlocks = newCarryoverBlocks;
      if ( ! solrUpdates.isEmpty() )
        RecordsToSolr.updateBibsInSolr( voyager, inventoryDB , solrUpdates );
      else
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {  }
      Timestamp ctime = minCarryOverTime(carryoverExpectedChanges);
      time = newTime;
      if (ctime != null && ctime.before(time))
        RecordsToSolr.setCurrentToDate( ctime , inventoryDB, CURRENT_TO_KEY );
      else
        RecordsToSolr.setCurrentToDate(  time , inventoryDB, CURRENT_TO_KEY );
    }
  }

  private static Timestamp minCarryOverTime(Map<Integer,Set<Change>> changes) {
    Timestamp min = null;
    for (Set<Change> bibChanges : changes.values())
      for (Change c : bibChanges)
        if ( min == null || c.changeDate.before(min))
          min = c.changeDate;
    if (min != null) {
      long ageInSeconds = Duration.between(min.toInstant(), Instant.now()).getSeconds();
      if (ageInSeconds > 60) {
        System.out.printf("Slowdown - %s%02d:%02ds\n",
            ((ageInSeconds > 3600)?(ageInSeconds/3600+":"):""),
                (ageInSeconds % 3600) / 60,ageInSeconds % 60);
      }
    }
    return min;
  }

  private static void addChangeToSet(Map<Integer, Set<Change>> changeSet, Integer bibId, Change c) {
    if (changeSet.containsKey(bibId)) {
      changeSet.get(bibId).add(c);
      return;
    }
    Set<Change> t = new HashSet<>();
    t.add(c);
    changeSet.put(bibId, t);
  }

  private static void addCarriedoverExpectedChangesToFoundChanges(
      Map<Integer, Set<Change>> main, Map<Integer, Set<Change>> adds) {
    if (adds.isEmpty()) return;
    for (Entry<Integer,Set<Change>> e : adds.entrySet())
      if (main.containsKey(e.getKey()))
        main.get(e.getKey()).addAll(e.getValue());
      else
        main.put(e.getKey(), e.getValue());
  }

  private static Map<Change.Type,String> recordDateQueries = new HashMap<>();
  static {
    recordDateQueries.put(Change.Type.BIB, "SELECT record_date FROM bibRecsSolr WHERE bib_id = ?");
    recordDateQueries.put(Change.Type.HOLDING, "SELECT record_date FROM mfhdRecsSolr WHERE mfhd_id = ?");
    recordDateQueries.put(Change.Type.ITEM, "SELECT record_date FROM itemRecsSolr WHERE item_id = ?");
  }

  private static boolean changeIsMet(Connection inventoryDB, Change c) throws SQLException {
    try ( PreparedStatement pstmt = inventoryDB.prepareStatement(recordDateQueries.get(c.type) )) {
      pstmt.setInt(1, c.recordId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          Timestamp t = rs.getTimestamp(1);
          if ( t == null || t.before(new Timestamp(c.changeDate.getTime()-1000)) )
            return false;

          // If found Solr document is based on a record change only a second older than expected,
          // accept that update as fulfilling the expected change only after a minute has passed.
          // TODO This exemption should be removed once record dates are tracked exclusively using
          // the db table timestamps, as opposed to a mix of these and the dates internal to the
          // MARC, which can create occasional discrepancies.
          if ( t.before(new Timestamp(c.changeDate.getTime())) )
              return Duration.between(t.toInstant(), Instant.now()).getSeconds() > 180;
        }
        return true;
      }
    }
  }
}
