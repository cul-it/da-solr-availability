package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
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
  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, XMLStreamException, SolrServerException {
    Timestamp since = Timestamp.valueOf("2018-04-05 17:00:00.0");

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

      Map<Integer,Set<Change>> newlyChangedBibs = RecordsToSolr.eliminateCarryovers(
          RecordsToSolr.duplicateMap(changedBibs), carryoverChangeBlocks);

      addCarriedoverExpectedChangesToFoundChanges(newlyChangedBibs, carryoverExpectedChanges);

      carryoverExpectedChanges = setAsideChangesNotAvailableInInventory( inventoryDB, newlyChangedBibs );

      carryoverChangeBlocks = newlyChangedBibs;
      if ( newlyChangedBibs.size() > 0 )
        RecordsToSolr.updateBibsInSolr( voyager, inventoryDB , newlyChangedBibs );
      else
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {  }
      time = newTime; 
    }
  }

  private static void addCarriedoverExpectedChangesToFoundChanges(
      Map<Integer, Set<Change>> main, Map<Integer, Set<Change>> adds) {
    for (Entry<Integer,Set<Change>> e : adds.entrySet())
      if (main.containsKey(e.getKey()))
        main.get(e.getKey()).addAll(e.getValue());
      else
        main.put(e.getKey(), e.getValue());
  }

  // split changed bibs into those to apply to Solr and expectations to carry over
  private static Map<Integer, Set<Change>> setAsideChangesNotAvailableInInventory(
      Connection inventoryDB, Map<Integer, Set<Change>> changedBibs) {
    // TODO Auto-generated method stub
    return null;
  }

}
