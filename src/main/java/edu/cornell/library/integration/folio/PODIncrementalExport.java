package edu.cornell.library.integration.folio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import edu.cornell.library.integration.folio.PODExporter.UpdateType;

public class PODIncrementalExport {

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    String configFile = System.getenv("configFile");
    if ( configFile != null ) {
      System.out.println(configFile);
      File f = new File(configFile);
      if (f.exists()) {
        try ( InputStream is = new FileInputStream(f) ) { prop.load( is ); }
      } else System.out.println("File does not exist: "+configFile);
    } else
      try (InputStream in = Thread.currentThread().getContextClassLoader()
         .getResourceAsStream("database.properties")){ prop.load(in); }

    try (Connection inventory = DriverManager.getConnection(prop.getProperty("inventoryDBUrl"),
        prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass")) ){

      OkapiClient okapi = new OkapiClient(
          prop.getProperty("okapiUrlFolio"),prop.getProperty("okapiTokenFolio"),
          prop.getProperty("okapiTenantFolio"));

      PODExporter exporter = new PODExporter( inventory, okapi );

      Set<String> bibs = identifyChangedRecords(inventory);
      Calendar cal = Calendar.getInstance();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      String today = sdf.format(cal.getTime());

      String updatesFile = String.format("cornell-incr-%s.xml", today);
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(updatesFile));
      writer.write("<?xml version='1.0' encoding='UTF-8'?>"
          + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
      int updateRecords = 0;

      String deletesFile = String.format("cornell-deletes-%s.txt", today);
      BufferedWriter deletes = Files.newBufferedWriter(Paths.get(deletesFile));
      int deleteRecords = 0;

      for ( String bibId : bibs ) {
        UpdateType isUpdate = exporter.exportBib(bibId, writer, deletes);
        switch (isUpdate) {
        case UPDATE: updateRecords++; break;
        case DELETE: deleteRecords++; break;
        default: continue;
        }
      }

      if ( updateRecords > 0 ) {
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
        String gzipFile = gzip( updatesFile );
      }
      deletes.flush();
      deletes.close();
      System.out.printf("Updates and new MARC: %d\n", updateRecords);
      System.out.printf("Deletes: %d\n", deleteRecords);
    }

  }

  private static String gzip(String file) throws IOException {
    String gzipFile = file+".gz";
    try (
        FileInputStream fis = new FileInputStream(file);
        FileOutputStream fos = new FileOutputStream(gzipFile);
        GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
        ){

        byte[] buffer = new byte[1024];
        int len;
        while((len=fis.read(buffer)) != -1){ gzipOS.write(buffer, 0, len); }
    }
    return gzipFile;
}


  private static Set<String> identifyChangedRecords(Connection inventory) throws SQLException {

    Set<String> changedInstances = new HashSet<>();
    String dateCursor = "2022-06-11";

    // NEWER IN FOLIO CACHE

    String newerInstanceQuery =
    "SELECT p.instanceHrid"+
    "  FROM bibPod p,"+
    "       (SELECT f.hrid, f.moddate"+
    "          FROM instanceFolio f"+
    "         WHERE f.moddate > ?) AS f1"+
    " WHERE p.instanceHrid = f1.hrid"+
    "   AND f1.moddate > p.instanceModdate";
    changedInstances.addAll(getChanged(inventory,newerInstanceQuery,dateCursor));

    String newerBibQuery =
    "SELECT p.instanceHrid"+
    "  FROM bibPod p,"+
    "       (SELECT f.instanceHrid, f.moddate"+
    "          FROM bibFolio f"+
    "         WHERE f.moddate > ?) AS f1"+
    " WHERE p.instanceHrid = f1.instanceHrid"+
    "   AND f1.moddate > p.moddate";
    changedInstances.addAll(getChanged(inventory,newerBibQuery,dateCursor));

    String newerHoldingQuery =
    "SELECT p.instanceHrid, f1.instanceHrid"+
    "  FROM holdingPod p,"+
    "       (SELECT f.hrid, f.moddate, f.instanceHrid"+
    "          FROM holdingFolio f"+
    "         WHERE f.moddate > ?) AS f1"+
    " WHERE p.hrid = f1.hrid"+
    "   AND f1.moddate > p.moddate";
    changedInstances.addAll(getChanged(inventory,newerHoldingQuery,dateCursor));

    String newerItemQuery =
    "SELECT p2.instanceHrid, f2.instanceHrid"+
    "  FROM itemPod p,"+
    "       (SELECT f.hrid, f.moddate, f.holdingHrid"+
    "          FROM itemFolio f"+
    "         WHERE f.moddate > ?) AS f1,"+
    "       holdingPod p2,"+
    "       holdingFolio f2"+
    " WHERE p.hrid = f1.hrid"+
    "   AND f1.moddate > p.moddate"+
    "   AND p.holdingHrid = p2.hrid"+
    "   AND f1.holdingHrid = f2.hrid";
    changedInstances.addAll(getChanged(inventory,newerItemQuery,dateCursor));
    System.out.println("All updates: "+changedInstances.size());

    changedInstances.addAll(getNewAndDeleted(inventory));
    System.out.println("All changes: "+changedInstances.size());

    return changedInstances;
  }

  private static Set<String> getNewAndDeleted(Connection inventory) throws SQLException {

    Set<String> affectedInstances = new HashSet<>();

    try ( Statement stmt = inventory.createStatement() ) {
      BIBS: {
        Set<String> allFolioBibs = getAll( stmt, "SELECT instanceHrid FROM bibFolio");
        Set<String> allPodBibs = getAll( stmt, "SELECT instanceHrid FROM bibPod");
        Set<String> newBibs = new HashSet<>(allFolioBibs);
        newBibs.removeAll(allPodBibs);
        System.out.println("total new bibs: "+newBibs.size());
        affectedInstances.addAll(newBibs);
        allPodBibs.removeAll(allFolioBibs);
        System.out.println("total deleted bibs: "+allPodBibs.size());
        affectedInstances.addAll(allPodBibs);
        allFolioBibs.clear();allPodBibs.clear();newBibs.clear();
      }

    HOLDINGS: {
        Map<String,String> allFolioHoldings =
            getAllMaps( stmt, "SELECT hrid, instanceHrid FROM holdingFolio");
        Map<String,String> allPodHoldings =
            getAllMaps( stmt, "SELECT hrid, instanceHrid FROM holdingPod");
        Map<String,String> newHoldings = new HashMap<>(allFolioHoldings);
        newHoldings.keySet().removeAll(allPodHoldings.keySet());
        System.out.println("total new holdings: "+newHoldings.size());
        affectedInstances.addAll(newHoldings.values());
        allPodHoldings.keySet().removeAll(allFolioHoldings.keySet());
        System.out.println("total deleted holdings: "+allPodHoldings.size());
        affectedInstances.addAll(allPodHoldings.values());
        allFolioHoldings.clear();allPodHoldings.clear();newHoldings.clear();
      }


      ITEMS: {
          Map<String,String> allFolioItems =
              getAllMaps( stmt,
               "SELECT i.hrid, instanceHrid"+
               "  FROM itemFolio i, holdingFolio h"+
               " WHERE i.holdingHrid = h.hrid");
          Map<String,String> allPodItems =
              getAllMaps( stmt,
               "SELECT i.hrid, instanceHrid"+
               "  FROM itemPod i, holdingPod h"+
               " WHERE i.holdingHrid = h.hrid");
          Map<String,String> newItems = new HashMap<>(allFolioItems);
          newItems.keySet().removeAll(allPodItems.keySet());
          System.out.println("total new items: "+newItems.size());
          affectedInstances.addAll(newItems.values());
          allPodItems.keySet().removeAll(allFolioItems.keySet());
          System.out.println("total deleted items: "+allPodItems.size());
          affectedInstances.addAll(allPodItems.values());
          allFolioItems.clear();allPodItems.clear();newItems.clear();
        }
    }
    return affectedInstances;
  }

  private static Set<String> getChanged(
      Connection inventory, String query, String dateCursor) throws SQLException {
    Set<String> changes = new HashSet<>();
    try (PreparedStatement stmt = inventory.prepareStatement(query)) {
      stmt.setString(1, dateCursor);
      try (ResultSet rs = stmt.executeQuery()) {
        int columns = rs.getMetaData().getColumnCount();
        while (rs.next()) for (int i = 1; i <= columns; i++) changes.add(rs.getString(i));
      }
    }
    return changes;
  }


  private static Map<String,String> getAllMaps(Statement stmt, String query) throws SQLException {
    Map<String,String> recs = new HashMap<>();
    try ( ResultSet rs = stmt.executeQuery(query) ) {
      while (rs.next()) recs.put(rs.getString(1),rs.getString(2));
    }
    return recs;
  }

  private static Set<String> getAll(Statement stmt, String query) throws SQLException {
    Set<String> recs = new HashSet<>();
    try ( ResultSet rs = stmt.executeQuery(query) ) {
      while (rs.next()) recs.add(rs.getString(1));
    }
    return recs;
  }

}
