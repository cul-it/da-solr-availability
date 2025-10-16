package edu.cornell.library.integration.availability;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.client.solrj.impl.Http2SolrClient;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    Map<String, String> env = System.getenv();
    String configFile = env.get("configFile");
    if (configFile == null)
      throw new IllegalArgumentException("configFile must be set in environment to valid file path.");
    Properties prop = new Properties();
    File f = new File(configFile);
    if (f.exists()) {
      try ( InputStream is = new FileInputStream(f) ) { prop.load( is ); }
    } else System.out.println("File does not exist: "+configFile);

    System.out.println( "Deleting records from Solr indexes at:" );
    System.out.println(prop.getProperty("solrUrl")+"/"+prop.getProperty("blacklightSolrCore"));
    System.out.println(prop.getProperty("solrUrl")+"/"+prop.getProperty("callnumSolrCore"));


    try ( Connection inventoryDB = DriverManager.getConnection(
        prop.getProperty("databaseURLCurrent"),
        prop.getProperty("databaseUserCurrent"),
        prop.getProperty("databasePassCurrent"));
        PreparedStatement queryQ = inventoryDB.prepareStatement
            ("SELECT hrid FROM deleteQueue ORDER BY priority LIMIT 100");
        PreparedStatement folioCacheCheck = inventoryDB.prepareStatement
            ("SELECT content FROM instanceFolio WHERE hrid = ?");
        PreparedStatement deleteFromQ = inventoryDB.prepareStatement
            ("DELETE FROM deleteQueue WHERE hrid = ?");
        PreparedStatement deleteFromGenQ = inventoryDB.prepareStatement
            ("DELETE FROM generationQueue WHERE hrid = ?");
        PreparedStatement deleteFromAvailQ = inventoryDB.prepareStatement
            ("DELETE FROM availQueue WHERE hrid = ?");
        PreparedStatement getTitle = inventoryDB.prepareStatement
            ("SELECT title FROM bibRecsSolr WHERE bib_id = ?");
        PreparedStatement deleteFromBRS = inventoryDB.prepareStatement
            ("UPDATE bibRecsSolr SET active = 0 WHERE bib_id = ?");
        PreparedStatement deleteFromMRS = inventoryDB.prepareStatement
            ("DELETE FROM mfhdRecsSolr WHERE mfhd_id = ?");
        PreparedStatement deleteFromIRS = inventoryDB.prepareStatement
            ("DELETE FROM itemRecsSolr WHERE mfhd_id = ?");
        PreparedStatement deleteFromPMD = inventoryDB.prepareStatement
            ("DELETE FROM processedMarcData WHERE hrid = ?");
        PreparedStatement getHoldingIds = inventoryDB.prepareStatement
            ("SELECT mfhd_id FROM mfhdRecsSolr WHERE bib_id = ?");
        PreparedStatement queueHeadingsUpdate = inventoryDB.prepareStatement
            ("INSERT INTO headingsQueue (hrid,priority,cause,record_date ) VALUES (?,5,'Delete all',now())");
        Statement stmt = inventoryDB.createStatement();
        Http2SolrClient solr = new Http2SolrClient
            .Builder(prop.getProperty("solrUrl")+"/"+prop.getProperty("blacklightSolrCore"))
            .withBasicAuthCredentials(prop.getProperty("solrUser"),prop.getProperty("solrPassword")).build();
        Http2SolrClient callNumberSolr = new Http2SolrClient
            .Builder(prop.getProperty("solrUrl")+"/"+prop.getProperty("callnumSolrCore"))
            .withBasicAuthCredentials(prop.getProperty("solrUser"),prop.getProperty("solrPassword")).build();
        ) {

      int countFound = 0;
      do {

        List<String> bibIds = new ArrayList<>();
        countFound = 0;

        try ( ResultSet rs = queryQ.executeQuery() ) { while ( rs.next() ) {

          String bibId = rs.getString(1);

          String title = null;
          getTitle.setInt(1,Integer.valueOf(bibId));
          try (ResultSet rs1 = getTitle.executeQuery() ) {while (rs1.next()) title = rs1.getString(1);}

          if ( checkActiveInFolio( folioCacheCheck, bibId ) ) {
            System.out.printf("Marked for deletion, but now active. Dequeuing %d: (%s)\n",bibId,title);
            deleteFromQ.setString(1,bibId);
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
          deleteFromBRS.setInt(1, Integer.valueOf(bibId));
          deleteFromBRS.addBatch();
          deleteFromPMD.setString(1, bibId);
          deleteFromPMD.addBatch();
          Set<Integer> holdingIds = new HashSet<>();
          getHoldingIds.setInt(1,Integer.valueOf(bibId));
          try (ResultSet rs1 = getHoldingIds.executeQuery()) {
            while (rs1.next()) holdingIds.add(rs1.getInt(1));}
          for ( Integer holdingId : holdingIds ) {
            deleteFromMRS.setInt(1, holdingId);
            deleteFromMRS.addBatch();
            deleteFromIRS.setInt(1, holdingId);
            deleteFromIRS.addBatch();
          }

          // Delete solr Fields Data
          deleteFromPMD.setString(1, bibId);
          deleteFromPMD.addBatch();

          // Delete from Delete Queue
          deleteFromQ.setString(1,bibId);
          deleteFromQ.addBatch();
  
          // Delete from other queues
          deleteFromGenQ.setString(1,bibId);
          deleteFromGenQ.addBatch();
          deleteFromAvailQ.setString(1,bibId);
          deleteFromAvailQ.addBatch();

          // Queue headings counts update
          queueHeadingsUpdate.setString(1, bibId);
          queueHeadingsUpdate.addBatch();
        }}

        WorksAndInventory.deleteWorkRelationships( inventoryDB, bibIds );

        deleteFromQ.executeBatch();
        deleteFromGenQ.executeBatch();
        deleteFromAvailQ.executeBatch();
        deleteFromBRS.executeBatch();
        deleteFromPMD.executeBatch();
        deleteFromMRS.executeBatch();
        deleteFromIRS.executeBatch();
        deleteFromPMD.executeBatch();
        queueHeadingsUpdate.executeBatch();
        System.out.println( countFound+" deleted");
      } while ( countFound > 0 );
    }
  }

  private static boolean checkActiveInFolio(PreparedStatement folioCacheCheck, String bibId)
      throws IOException, SQLException {
    folioCacheCheck.setString(1, bibId);
    try ( ResultSet rs = folioCacheCheck.executeQuery() ) {
      while ( rs.next() ) {
        Map<String,Object> instance = mapper.readValue(rs.getString(1), Map.class);
        if (instance.containsKey("discoverySuppress")) {
          Object o = instance.get("discoverySuppress");
          if (String.class.isInstance(o)) {
            if (((String)o).equals("true")) return false;
          } else if ((Boolean)o) return false;
        }
        if (instance.containsKey("staffSuppress")) {
          Object o = instance.get("staffSuppress");
          if (String.class.isInstance(o)) {
            if (((String)o).equals("true")) return false;
          } else if ((Boolean)o) return false;
        }
        return true; // record found, neither suppression flag set
      }
    }
    return false; // record not found, therefore not active
  }
  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

}
