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
import java.util.Properties;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Items;

public class MonitorAvailability {

  private static final String CURRENT_TO_KEY = "avail";

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException, XMLStreamException, SolrServerException, InterruptedException {
    Timestamp since = Timestamp.valueOf("2018-05-01 16:45:00.0");

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Class.forName("com.mysql.jdbc.Driver");

    try (Connection voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"))) {

      since = RecordsToSolr.getCurrentToDate( since, inventoryDB, CURRENT_TO_KEY );

      Timestamp time = since;
      System.out.println(time);
         Map<Integer,Set<Change>> carryoverChanges = new HashMap<>();
         while ( true ) {
           Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-6000);
           Map<Integer,Set<Change>> changedBibs = Items.detectChangedItemStatuses(voyagerLive, time);
           Map<Integer,Set<Change>> newlyChangedBibs = RecordsToSolr.eliminateCarryovers(
               RecordsToSolr.duplicateMap(changedBibs), carryoverChanges);
           carryoverChanges = changedBibs;
           if ( newlyChangedBibs.size() > 0 )
             RecordsToSolr.updateBibsInSolr( voyagerLive, inventoryDB , newlyChangedBibs );
           else
             try {
               Thread.sleep(500);
             } catch (InterruptedException e) {  }
           time = newTime;
           RecordsToSolr.setCurrentToDate( time, inventoryDB, CURRENT_TO_KEY );
         }
    }
  }

}
