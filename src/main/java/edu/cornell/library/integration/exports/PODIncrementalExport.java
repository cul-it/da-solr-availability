package edu.cornell.library.integration.exports;

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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import javax.naming.AuthenticationException;

import edu.cornell.library.integration.exports.PODExporter.UpdateType;
import edu.cornell.library.integration.folio.FolioClient;


public class PODIncrementalExport {

  public static void main(String[] args) throws IOException, SQLException, AuthenticationException {

    Map<String, String> env = System.getenv();
    String configFile = env.get("configFile");
    if (configFile == null)
      throw new IllegalArgumentException("configFile must be set in environment to valid file path.");
    Properties prop = new Properties();
    File f = new File(configFile);
    if (f.exists()) {
      try ( InputStream is = new FileInputStream(f) ) { prop.load( is ); }
    } else System.out.println("File does not exist: "+configFile);

    try (Connection inventory = DriverManager.getConnection(prop.getProperty("databaseURLCurrent"),
        prop.getProperty("databaseUserCurrent"),prop.getProperty("databasePassCurrent")) ){

      FolioClient folio = new FolioClient(prop,"Folio");

      PODExporter exporter = new PODExporter( inventory, folio, prop, true );
      exporter.verbose = true;


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
        System.out.println("pushing update file");
        exporter.pushFileToPod( gzipFile, "application/gzip")
      }
      deletes.flush();
      deletes.close();
      System.out.printf("Updates and new MARC: %d\n", updateRecords);
      System.out.printf("Deletes: %d\n", deleteRecords);
      if ( deleteRecords > 0 ) {
        System.out.println("pushing delete file");
        exporter.pushFileToPod( deletesFile, "text/plain");
      }

      exporter.executeDelayedCommits();
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
//    Timestamp since = Timestamp.valueOf(LocalDateTime.now().minusMonths(1));
    Timestamp since = Timestamp.valueOf(LocalDateTime.now().minusHours(1 ));

    System.out.println("Looking for changed instances");
    changedInstances.addAll(getNewer(inventory,since,
        "SELECT hrid, moddate FROM instanceFolio WHERE moddate > ?",
        "SELECT instanceModdate FROM instancePod WHERE instanceHrid = ?"));

    System.out.println("Looking for changed bibs");
    changedInstances.addAll(getNewer(inventory,since,
        "SELECT instanceHrid, moddate FROM bibFolio WHERE moddate > ?",
        "SELECT bibModdate FROM instancePod WHERE instanceHrid = ?"));

    System.out.println("Looking for changed holdings");
    changedInstances.addAll(getNewerHoldings(inventory,since));

    System.out.println("Looking for changed items");
    changedInstances.addAll(getNewerItems(inventory,since));

    System.out.println("Looking for deleted records");
    changedInstances.addAll(getDeleted(inventory));
    System.out.println("All changes: "+changedInstances.size());

    return changedInstances;
  }

  private static Set<String> getNewer(Connection inventory, Timestamp since, String folioQuery, String podQuery) throws SQLException {
    Set<String> changed = new HashSet<>();
    Map<String,Timestamp> folioRecs = getAllMaps(inventory, folioQuery, since);
    System.out.format("%d recently changed\n", folioRecs.size());
    try( PreparedStatement podQ = inventory.prepareStatement(podQuery)) {
      int count=0;
      for (String hrid : folioRecs.keySet()) {
        podQ.setString(1, hrid);
        try (ResultSet podRs = podQ.executeQuery()) {
          if (podRs.next()) {
            // rec is in pod; queue if newer
            if (podRs.getTimestamp(1).before(folioRecs.get(hrid))) {
              System.out.format("Record %s is newer %s; %s\n", hrid, folioRecs.get(hrid), podRs.getTimestamp(1));
              changed.add(hrid);
            }
          } else {
            // rec is not in pod
            System.out.format("Record %s is not in pod\n", hrid);
            changed.add(hrid);
          }
        }
        if (++count % 1000 == 0) System.out.format("%d evaluated\n", count);
      }
    }
    return changed;
  }

  private static Set<String> getNewerHoldings(Connection inventory, Timestamp since) throws SQLException {
    Set<String> changed = new HashSet<>();
    List<HoldingFolio> folioRecs = new ArrayList<>();
    try (PreparedStatement folioQ = inventory.prepareStatement(
        "SELECT instanceHrid, hrid, moddate FROM holdingFolio WHERE moddate > ?")){
      folioQ.setTimestamp(1, since);
      try (ResultSet folioRs = folioQ.executeQuery()) {
        while (folioRs.next()) folioRecs.add(new HoldingFolio(folioRs.getString(1),folioRs.getString(2),folioRs.getTimestamp(3)));
      }
    }
    System.out.format("%d recently changed\n", folioRecs.size());
    try( PreparedStatement podQ = inventory.prepareStatement(
        "SELECT instanceHrid, hrid, moddate FROM holdingPod WHERE hrid = ?")) {
      int count=0;
      for (HoldingFolio holdingFolio : folioRecs) {
        podQ.setString(1, holdingFolio.holdingHrid());
        try (ResultSet podRs = podQ.executeQuery()) {
          if (podRs.next()) {
            // rec is in pod; queue if newer
            Timestamp podModdate = podRs.getTimestamp("moddate");
            if (podModdate == null || podModdate.before(holdingFolio.moddate())) {
              System.out.format("Holding %s is newer %s; %s\n", holdingFolio.holdingHrid, holdingFolio.moddate(), podRs.getTimestamp("moddate"));
              changed.add(holdingFolio.instanceHrid());
              changed.add(podRs.getString("instanceHrid"));
            }
          } else {
            // rec is not in pod
            System.out.format("Holding %s is not in pod\n", holdingFolio.holdingHrid());
            changed.add(holdingFolio.instanceHrid());
          }
        }
        if (++count % 1000 == 0) System.out.format("%d evaluated\n", count);
      }
    }
    return changed;
  }
  record HoldingFolio(String instanceHrid, String holdingHrid, Timestamp moddate) { }

  private static Set<String> getNewerItems(Connection inventory, Timestamp since) throws SQLException {
    Set<String> changed = new HashSet<>();
    List<ItemFolio> folioRecs = new ArrayList<>();
    try (PreparedStatement folioQ = inventory.prepareStatement(
        "SELECT holdingHrid, hrid, moddate FROM itemFolio WHERE moddate > ?")){
      folioQ.setTimestamp(1, since);
      try (ResultSet folioRs = folioQ.executeQuery()) {
        while (folioRs.next()) folioRecs.add(new ItemFolio(folioRs.getString(1),folioRs.getString(2),folioRs.getTimestamp(3)));
      }
    }
    System.out.format("%d recently changed\n", folioRecs.size());
    try( PreparedStatement podQ = inventory.prepareStatement(
        "SELECT holdingHrid, hrid, moddate FROM itemPod WHERE hrid = ?")) {
      int count=0;
      for (ItemFolio itemFolio : folioRecs) {
        podQ.setString(1, itemFolio.itemHrid());
        try (ResultSet podRs = podQ.executeQuery()) {
          if (podRs.next()) {
            // rec is in pod; queue if newer
            Timestamp podModdate = podRs.getTimestamp("moddate");
            if (podModdate == null || podModdate.before(itemFolio.moddate())) {
              System.out.format("Item %s is newer %s; %s\n", itemFolio.itemHrid(), itemFolio.moddate(), podRs.getTimestamp("moddate"));
              changed.add(getInstanceHrid(inventory,itemFolio.holdingHrid()));
              changed.add(getInstanceHrid(inventory,podRs.getString("holdingHrid")));
            }
          } else {
            // rec is not in pod
            System.out.format("Item %s is not in pod\n", itemFolio.itemHrid());
            changed.add(getInstanceHrid(inventory,itemFolio.holdingHrid()));
          }
        }
        if (++count % 1000 == 0) System.out.format("%d evaluated\n", count);
      }
    }
    return changed;
  }
  private static String getInstanceHrid(Connection inventory, String holdingHrid) throws SQLException {
    try (PreparedStatement q = inventory.prepareStatement("SELECT instanceHrid FROM holdingFolio WHERE hrid = ?")){
      q.setString(1, holdingHrid);
      try (ResultSet rs = q.executeQuery()){
        while (rs.next()) return rs.getString(1);
      }
    }
    return null;
  }
  record ItemFolio(String holdingHrid, String itemHrid, Timestamp moddate) { }

  private static Set<String> getDeleted(Connection inventory) throws SQLException {

    Set<String> affectedInstances = new HashSet<>();

    try ( Statement stmt = inventory.createStatement() ) {
      INSTANCES: {
        Set<String> allFolioBibs = getAll( stmt, "SELECT hrid FROM instanceFolio");
        Set<String> allPodBibs = getAll( stmt, "SELECT instanceHrid FROM instancePod");
        allPodBibs.removeAll(allFolioBibs);
        System.out.println("total deleted bibs: "+allPodBibs.size());
        if ( ! allPodBibs.isEmpty()) System.out.println(allPodBibs);
        affectedInstances.addAll(allPodBibs);
      }

      HOLDINGS: {
        Set<String> allFolioHoldings = getAll( stmt, "SELECT hrid FROM holdingFolio");
        Map<String,String> allPodHoldings =
            getAllMaps( stmt, "SELECT hrid, instanceHrid FROM holdingPod");
        allPodHoldings.keySet().removeAll(allFolioHoldings);
        System.out.println("total deleted holdings: "+allPodHoldings.size());
        System.out.println(allPodHoldings);
        affectedInstances.addAll(allPodHoldings.values());
      }


      ITEMS: {
          Set<String> allFolioItems = getAll( stmt, "SELECT hrid FROM itemFolio");
          Map<String,String> allPodItems = getAllMaps( stmt, "SELECT hrid, holdingHrid FROM itemPod i");
          allPodItems.keySet().removeAll(allFolioItems);
          System.out.println("total deleted items: "+allPodItems.size());
          for (String itemHrid : allPodItems.keySet()) {
            String holdingHrid = allPodItems.get(itemHrid);
            String instanceHrid = getInstanceHrid(inventory,holdingHrid);
            if (instanceHrid == null) {
              System.out.format("item: %s; holding %s - not traced back to instance\n",itemHrid, holdingHrid);
              continue;
            }
            System.out.format("item: %s; holding %s; instance: %s\n",itemHrid, holdingHrid, instanceHrid);
            affectedInstances.add(instanceHrid);
          }
        }
    }
    return affectedInstances;
  }

  /**
   * Execute query with a timestamp parameter, and return HashMap of String and Timestamp built from columns
   * one and two of the results. */
  private static Map<String,Timestamp> getAllMaps(Connection db, String query, Timestamp param) throws SQLException {
    Map<String,Timestamp> folioRecs = new HashMap<>();
    try (PreparedStatement folioQ = db.prepareStatement(query)){
      folioQ.setTimestamp(1, param);
      try (ResultSet folioRs = folioQ.executeQuery()) {
        while (folioRs.next()) folioRecs.put(folioRs.getString(1), folioRs.getTimestamp(2));
      }
    }
    return folioRecs;
  }

  /** Use existing Statement to execute query, and return HashMap of String and String build from first
   *  two colums of the results. */
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
