package edu.cornell.library.integration.changes;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;

public class MonitorIndexQueue {

  private static final String CURRENT_TO_KEY = "queue";

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, SQLException, InterruptedException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    Class.forName("com.mysql.jdbc.Driver");
    try ( Connection inventoryDB = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement pstmt = inventoryDB.prepareStatement
            ("SELECT bib_id, cause FROM solrInventory.indexQueue WHERE done_date > ?");
        PreparedStatement generationQueue = inventoryDB.prepareStatement
            ("INSERT INTO generationQueue (bib_id, cause, record_date, priority) VALUES (?,?,NOW(),0)");
        PreparedStatement deleteQueue = inventoryDB.prepareStatement
            ("INSERT INTO deleteQueue (bib_id, cause, record_date, priority)"+
            " VALUES (?,'Record Deleted or Suppressed',NOW(),0)");
        PreparedStatement availabilityQueue = inventoryDB.prepareStatement
            ("INSERT INTO availabilityQueue (bib_id, cause, record_date, priority) VALUES (?,?,NOW(),?)")){

      Timestamp time = Change.getCurrentToDate( inventoryDB, CURRENT_TO_KEY );
      if (time == null) {
        time = new Timestamp(Calendar.getInstance().getTime().getTime()-(10*60*60*1000));
        System.out.println("No starting timestamp in DB, defaulting to 10 hours ago.");
      }
      System.out.println(time);

      do {

        Timestamp newTime = new Timestamp(Calendar.getInstance().getTime().getTime()-1000);
        pstmt.setTimestamp(1, time);

        int i = 0;
        try ( ResultSet rs = pstmt.executeQuery()) {
          while ( rs.next() ) {
            int bibId = rs.getInt(1);
            String cause = rs.getString(2);
            if ( cause.equals("Age of Record in Solr") )
              continue;
            if ( cause.contains("Delete") ) {
              System.out.println(bibId+" deleted ("+cause+")");
              deleteQueue.setInt(1, bibId);
              deleteQueue.addBatch();
            } else if ( cause.contains("Item") ) {
              System.out.println(bibId+" item updated ("+cause+")");
              availabilityQueue.setInt(1, bibId);
              availabilityQueue.setString(2, cause);
              availabilityQueue.setInt(3, 5);
              availabilityQueue.addBatch();
           } else if ( cause.contains("Link") ) {
              System.out.println(bibId+" title link  ("+cause+")");
              availabilityQueue.setInt(1, bibId);
              availabilityQueue.setString(2, cause);
              availabilityQueue.setInt(3, 7);
              availabilityQueue.addBatch();
            } else {
              System.out.println(bibId+" record updated ("+cause+")");
              generationQueue.setInt(1, bibId);
              generationQueue.setString(2, cause);
              generationQueue.addBatch();
            }
            if (++i > 1000) {
              deleteQueue.executeBatch();
              availabilityQueue.executeBatch();
              generationQueue.executeBatch();
              i = 0;
            }
          }
        }

        deleteQueue.executeBatch();
        availabilityQueue.executeBatch();
        generationQueue.executeBatch();
        Thread.sleep(500);

        time = newTime;
        Change.setCurrentToDate( time, inventoryDB, CURRENT_TO_KEY );
      } while(true);
    }
  }
}
