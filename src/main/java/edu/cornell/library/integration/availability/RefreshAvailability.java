package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;

public class RefreshAvailability {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException, XMLStreamException, InterruptedException, SolrServerException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("oracle.jdbc.driver.OracleDriver");
    Class.forName("com.mysql.jdbc.Driver");

    try (
        Connection voyagerLive = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        SolrClient solr = new HttpSolrClient( System.getenv("SOLR_URL"));
        SolrClient callNumberSolr = new HttpSolrClient( System.getenv("CALLNUMBER_SOLR_URL"))) {

      int rows = 50;
      SolrQuery q = new SolrQuery().setQuery("*:*").addSort("timestamp", ORDER.asc).setFields("id,timestamp").setRows(rows);

      int page = 0;

      while ( true ) {

        q.setStart(page * rows);
        Map<Integer,Set<Change>> oldBibs = new TreeMap<>();
        for (SolrDocument doc : solr.query(q).getResults()) {
          Integer bibId = Integer.valueOf((String)doc.getFieldValue("id"));
          Set<Change> t = new HashSet<>();
          t.add(new Change(Change.Type.AGE,bibId,"Updating Availability",
              new Timestamp(((Date)doc.getFieldValue("timestamp")).getTime()),null));
          oldBibs.put(bibId,t);
        }
        page = (page + 1) % 10;

        if ( oldBibs.size() > 0 )
          RecordsToSolr.updateBibsInSolr( voyagerLive, inventoryDB ,solr, callNumberSolr, oldBibs );
        else
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            System.exit(0);
          }

      }

    }
  }

}
