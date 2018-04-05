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
import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Items;

public class MonitorRecordChanges {
  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, XMLStreamException, SolrServerException {
    Timestamp since = Timestamp.valueOf("2018-04-04 18:30:00.0");

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
    while ( true ) {
      Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-120_000);
      Map<Integer,Set<Change>> changedBibs = 
          Items.detectChangedItems(voyagerLive, time, new HashMap<Integer,Set<Change>>() );
      Holdings.detectChangedHoldings(voyagerLive, time, changedBibs);
      BibliographicSummary.detectChangedBibs(voyagerLive, time, changedBibs);
      if ( changedBibs.size() > 0 )
        RecordsToSolr.updateBibsInSolr( voyagerLive, inventoryDB , changedBibs );
      else
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {  }
      time = newTime; 
    }
  }

}
