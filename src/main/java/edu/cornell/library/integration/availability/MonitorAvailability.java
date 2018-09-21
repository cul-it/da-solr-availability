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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.RecentIssues;

public class MonitorAvailability {

  private static final String CURRENT_TO_KEY = "avail";

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException, XMLStreamException, InterruptedException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Class.forName("com.mysql.jdbc.Driver");

    try (Connection voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        SolrClient solr = new HttpSolrClient( System.getenv("SOLR_URL"));
        SolrClient callNumberSolr = new HttpSolrClient( System.getenv("CALLNUMBER_SOLR_URL"))) {

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
            Items.detectChangedItemStatuses(voyagerLive, time, new HashMap<Integer,Set<Change>>());
        RecentIssues.detectNewReceiptBibs(voyagerLive, time, changedBibs);
        Map<Integer,Set<Change>> newlyChangedBibs = RecordsToSolr.eliminateCarryovers(
            RecordsToSolr.duplicateMap(changedBibs), carryoverChanges);
        carryoverChanges = changedBibs;
        if ( newlyChangedBibs.size() > 0 )
          RecordsToSolr.updateBibsInSolr( voyagerLive, inventoryDB , solr, callNumberSolr, newlyChangedBibs );
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
