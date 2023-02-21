package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.availability.ProcessAvailabilityQueue.BibToUpdate;
import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.folio.OkapiClient;

public class RefreshAvailability {
  public static void main(String[] args)
      throws IOException, SQLException, SolrServerException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }

    try (
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        SolrClient solr = new HttpSolrClient( System.getenv("SOLR_URL"));
        SolrClient callNumberSolr = new HttpSolrClient( System.getenv("CALLNUMBER_SOLR_URL"))) {

      OkapiClient okapi = new OkapiClient(prop,"Folio");

      int rows = 50;
      SolrQuery q = new SolrQuery().setQuery("*:*").addSort("timestamp", ORDER.asc)
          .setFields("id,type,timestamp").setRows(rows);

      int page = 0;

      while ( true ) {

        q.setStart(page * rows);
        Set<BibToUpdate> oldBibs = new HashSet<>();
        for (SolrDocument doc : solr.query(q).getResults()) {
          String bibId = (String)doc.getFieldValue("id");
          Set<Change> t = new HashSet<>();
          t.add(new Change(Change.Type.AGE,bibId,"Updating Availability",
              new Timestamp(((Date)doc.getFieldValue("timestamp")).getTime()),null));
          oldBibs.add(new BibToUpdate(bibId,t ));
        }
        page = (page + 1) % 10;

        if ( oldBibs.size() > 0 )
//TODO Get up to speed with Folio mods          ProcessAvailabilityQueue.updateBibsInSolr( okapi, inventoryDB ,solr, callNumberSolr, oldBibs, 8 );
          System.out.println("This infrequently used, possibly obsolete tool is not current with the Folio code modifications.");
        else
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
          }

      }

    }
  }

}
