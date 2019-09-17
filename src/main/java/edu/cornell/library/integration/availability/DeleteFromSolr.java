package edu.cornell.library.integration.availability;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * Utility to delete bibs from Solr index.
 * 
 * Identifies bibs that should be deleted from solr based on the "Current"
 * database's CurrentDBTable.QUEUE table entries with `cause` listed as
 * DataChangeUpdateType.DELETE.
 * 
 * Bibs are deleted from Solr, and the "Current" database is updated to reflect
 * changes in the current Solr contents. Finally, any bibs that might link to
 * the deleted bibs based on shared work id's will be queued to be refreshed
 * in Solr by adding entries to the CurrentDBTable.QUEUE with
 * DataChangeUpdateType.TITLELINK.
 */
class DeleteFromSolr {

  public static void main(String[] argv) throws Exception{

    System.out.println( "Deleting records from Solr indexes at:" );
    System.out.println(System.getenv("SOLR_URL"));
    System.out.println(System.getenv("CALLNUMBER_SOLR_URL"));

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("com.mysql.jdbc.Driver");
    try ( Connection inventoryDB = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),
        prop.getProperty("inventoryDBUser"),
        prop.getProperty("inventoryDBPass"));
        PreparedStatement queryQ = inventoryDB.prepareStatement
            ("SELECT bib_id FROM deleteQueue ORDER BY priority LIMIT 100");
        PreparedStatement voyagerCheckQ = inventoryDB.prepareStatement
            ("SELECT active FROM bibRecsVoyager WHERE bib_id = ?");
        PreparedStatement deleteFromQ = inventoryDB.prepareStatement
            ("DELETE FROM deleteQueue WHERE bib_id = ?");
        PreparedStatement deleteFromGenQ = inventoryDB.prepareStatement
            ("DELETE FROM generationQueue WHERE bib_id = ?");
        PreparedStatement deleteFromAvailQ = inventoryDB.prepareStatement
            ("DELETE FROM availabilityQueue WHERE bib_id = ?");
        PreparedStatement getTitle = inventoryDB.prepareStatement
            ("SELECT title FROM bibRecsSolr WHERE bib_id = ?");
        PreparedStatement deleteFromBRS = inventoryDB.prepareStatement
            ("UPDATE bibRecsSolr SET active = 0 WHERE bib_id = ?");
        PreparedStatement deleteFromMRS = inventoryDB.prepareStatement
            ("DELETE FROM mfhdRecsSolr WHERE mfhd_id = ?");
        PreparedStatement deleteFromIRS = inventoryDB.prepareStatement
            ("DELETE FROM itemRecsSolr WHERE mfhd_id = ?");
        PreparedStatement getHoldingIds = inventoryDB.prepareStatement
            ("SELECT mfhd_id FROM mfhdRecsSolr WHERE bib_id = ?");
        Statement stmt = inventoryDB.createStatement();
        HttpSolrClient solr = new HttpSolrClient( System.getenv("SOLR_URL"));
        SolrClient callNumberSolr = new HttpSolrClient( System.getenv("CALLNUMBER_SOLR_URL")) ){

      int countFound = 0;
      do {

        List<Integer> bibIds = new ArrayList<>();
        countFound = 0;

        try ( ResultSet rs = queryQ.executeQuery() ) { while ( rs.next() ) {

          Integer bibId = rs.getInt(1);

          String title = null;
          getTitle.setInt(1,bibId);
          try (ResultSet rs1 = getTitle.executeQuery() ) {while (rs1.next()) title = rs1.getString(1);}

          if ( checkActiveInVoyager( voyagerCheckQ, bibId ) ) {
            System.out.printf("Marked for deletion, but now active. Dequeuing %d: (%s)\n",bibId,title);
            deleteFromQ.setInt(1,bibId);
            deleteFromQ.addBatch();
            continue;
          }
          bibIds.add(bibId);

          countFound++;

          System.out.println(bibId+" ("+title+")");

          // Delete from Solr
          solr.deleteById(bibId.toString());
          callNumberSolr.deleteByQuery("bibid:"+bibId);

          // Delete from Inventory tables
          deleteFromBRS.setInt(1, bibId);
          deleteFromBRS.addBatch();
          Set<Integer> holdingIds = new HashSet<>();
          getHoldingIds.setInt(1,bibId);
          try (ResultSet rs1 = getHoldingIds.executeQuery()) {while (rs1.next()) holdingIds.add(rs1.getInt(1));}
          for ( Integer holdingId : holdingIds ) {
            deleteFromMRS.setInt(1, holdingId);
            deleteFromMRS.addBatch();
            deleteFromIRS.setInt(1, holdingId);
            deleteFromIRS.addBatch();
          }

          // Delete from Delete Queue
          deleteFromQ.setInt(1,bibId);
          deleteFromQ.addBatch();
  
          // Delete from other queues
          deleteFromGenQ.setInt(1,bibId);
          deleteFromGenQ.addBatch();
          deleteFromAvailQ.setInt(1,bibId);
          deleteFromAvailQ.addBatch();
        }}

        WorksAndInventory.deleteWorkRelationships( inventoryDB, bibIds );

        deleteFromQ.executeBatch();
        deleteFromGenQ.executeBatch();
        deleteFromAvailQ.executeBatch();
        deleteFromBRS.executeBatch();
        deleteFromMRS.executeBatch();
        deleteFromIRS.executeBatch();
        System.out.println( countFound+" deleted");
      } while ( countFound > 0 );
      deleteFromQ.executeBatch();
      deleteFromGenQ.executeBatch();
      deleteFromAvailQ.executeBatch();
      deleteFromBRS.executeBatch();
      deleteFromMRS.executeBatch();
      deleteFromIRS.executeBatch();
    }
  }

  private static boolean checkActiveInVoyager(PreparedStatement voyagerCheckQ, Integer bibId) throws SQLException {
    voyagerCheckQ.setInt(1, bibId);
    try ( ResultSet rs = voyagerCheckQ.executeQuery() ) {
      while ( rs.next() ) return rs.getBoolean(1);
    }
    return false;
  }

}
