package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Items;

public class MonitorAvailability {

  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, XMLStreamException, SolrServerException {
    Timestamp since = Timestamp.valueOf("2018-04-06 09:40:00.0");

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Connection voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
    Class.forName("com.mysql.jdbc.Driver");
    Connection inventoryDB = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));

    Timestamp time = since;
    System.out.println(time);
    Map<Integer,Set<Change>> carryoverChanges = new HashMap<>();
    while ( true ) {
      Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-6000);
      Map<Integer,Set<Change>> changedBibs = Items.detectChangedItemStatuses(voyagerLive, time);
      Map<Integer,Set<Change>> newlyChangedBibs = eliminateCarryovers( duplicateMap(changedBibs), carryoverChanges);
      carryoverChanges = changedBibs;
      if ( newlyChangedBibs.size() > 0 )
        RecordsToSolr.updateBibsInSolr( voyagerLive, inventoryDB , newlyChangedBibs );
      else
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {  }
      time = newTime; 
    }
  }

  private static Map<Integer,Set<Change>> duplicateMap( Map<Integer,Set<Change>> m1 ) {
    Map<Integer,Set<Change>> m2 = new HashMap<>();
    for (Entry<Integer,Set<Change>> e : m1.entrySet())
      m2.put(e.getKey(), new HashSet<>(e.getValue()));
    return m2;
  }

  private static Map<Integer,Set<Change>> eliminateCarryovers( 
      Map<Integer,Set<Change>> newChanges, Map<Integer,Set<Change>> oldChanges) {
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
}
