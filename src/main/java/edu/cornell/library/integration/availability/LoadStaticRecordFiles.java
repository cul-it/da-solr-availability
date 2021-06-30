package edu.cornell.library.integration.availability;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LoadStaticRecordFiles {

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    String directory = prop.getProperty("staticRecordFileDirectory");

    try (Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement insertInstance = inventoryDB.prepareStatement(
            "INSERT INTO instanceFolio ( id, hrid, active, content) VALUES (?, ?, ?, ?)");
        PreparedStatement insertHolding = inventoryDB.prepareStatement(
            "INSERT INTO holdingFolio ( id, hrid, instanceHrid, active, content) VALUES (?, ?, ?, ?, ?)");
        PreparedStatement insertItem = inventoryDB.prepareStatement(
            "INSERT INTO itemFolio ( id, hrid, holdingHrid, content) VALUES (?, ?, ?, ?)");
        ){
      loadInstances( insertInstance, new File(directory+File.separatorChar+"instances.json"));
      loadInstances( insertInstance, new File(directory+File.separatorChar+"instances_suppressed.json"));
      Map<String,String> instanceMap = loadMapFile( directory, "instance_map_tabbed.txt");
      loadHoldings( insertHolding,  new File(directory+File.separatorChar+"holdings.json"), instanceMap);
      loadHoldings( insertHolding,  new File(directory+File.separatorChar+"holdings_suppressed.json"), instanceMap);
      instanceMap.clear();
      Map<String,String> holdingsMap = loadMapFile( directory, "holding_map_tabbed.txt");
      Map<String,String> itemMap = loadMapFile( directory, "item_map_tabbed.txt");
      loadItems( insertItem,  new File(directory+File.separatorChar+"items.json"), holdingsMap,itemMap);
    }
  }

  private static void loadItems(
      PreparedStatement insertItem, File file, Map<String, String> holdingsMap, Map<String, String> itemMap) throws SQLException, IOException {
    try ( BufferedReader r = new BufferedReader(new FileReader( file )) ) {
      String line;
      int count = 0;
      while ((line = r.readLine()) != null ) {
        System.out.println(line);
        String hrid = null;
        String holdingId = null;
        String holdingHrid = null;
        Matcher m = hridP.matcher(line);
        if ( m.matches() ) hrid = m.group(1) ;
        m = holdingIdP.matcher(line);
        if ( m.matches() ) {
          holdingId = m.group(1) ;
          if (holdingsMap.containsKey(holdingId))
            holdingHrid = holdingsMap.get(holdingId);
        }
        insertItem.setString(1, itemMap.get(hrid));
        insertItem.setString(2, hrid);
        insertItem.setString(3, holdingHrid);
        insertItem.setString(4, line);
        insertItem.addBatch();
        count++;
        if ( 0 == count % 500 ) {
          insertItem.executeBatch();
        }
      }
      insertItem.executeBatch();
    }
  }

  private static void loadHoldings(PreparedStatement insertInstance, File file, Map<String, String> instanceMap) throws SQLException, IOException {
    try ( BufferedReader r = new BufferedReader(new FileReader( file )) ) {
      String line;
      int count = 0;
      while ((line = r.readLine()) != null ) {
        System.out.println(line);
        String id = null;
        String hrid = null;
        String instanceId = null;
        String instanceHrid = null;
        boolean active = true;
        Matcher m = idP.matcher(line);
        if ( m.matches() ) id = m.group(1) ;
        m = hridP.matcher(line);
        if ( m.matches() ) hrid = m.group(1) ;
        m = instanceIdP.matcher(line);
        if ( m.matches() ) {
          instanceId = m.group(1) ;
          if (instanceMap.containsKey(instanceId))
            instanceHrid = instanceMap.get(instanceId);
        }
        m = suppressP.matcher(line);
        if ( m.matches() ) active = ! Boolean.valueOf( m.group(1) );
        insertInstance.setString(1, id);
        insertInstance.setString(2, hrid);
        insertInstance.setString(3, instanceHrid);
        insertInstance.setBoolean(4, active);
        insertInstance.setString(5, line);
        insertInstance.addBatch();
        count++;
        if ( 0 == count % 500 ) {
          insertInstance.executeBatch();
        }
      }
      insertInstance.executeBatch();
    }
  }

  private static void loadInstances(PreparedStatement insertHolding, File file) throws SQLException, IOException {
    try ( BufferedReader r = new BufferedReader(new FileReader( file )) ) {
      String line;
      int count = 0;
      while ((line = r.readLine()) != null ) {
        System.out.println(line);
        String id = null;
        String hrid = null;
        boolean active = true;
        Matcher m = idP.matcher(line);
        if ( m.matches() ) id = m.group(1) ;
        m = hridP.matcher(line);
        if ( m.matches() ) hrid = m.group(1) ;
        m = suppressP.matcher(line);
        if ( m.matches() ) active = ! Boolean.valueOf( m.group(1) );
        insertHolding.setString(1, id);
        insertHolding.setString(2, hrid);
        insertHolding.setBoolean(3, active);
        insertHolding.setString(4, line);
        insertHolding.addBatch();
        count++;
        if ( 0 == count % 500 ) {
          insertHolding.executeBatch();
        }
      }
      insertHolding.executeBatch();
    }
  }

  static Pattern instanceIdP = Pattern.compile("^.*\"instanceId\": *\"([0123456789abcdef-]+)\".*$");
  static Pattern holdingIdP = Pattern.compile("^.*\"holdingsRecordId\": *\"([0123456789abcdef-]+)\".*$");
  static Pattern idP = Pattern.compile("^.*\"id\": *\"([0123456789abcdef-]+)\".*$");
  static Pattern hridP = Pattern.compile("^.*\"hrid\": *\"([0123456789abcdef-]+)\".*$");
  static Pattern suppressP = Pattern.compile("^.*\"discoverySuppress\": *([falsetru]+),.*$");

  private static Map<String, String> loadMapFile(String directory, String file) throws IOException {
    List<String> lines = Files.readAllLines(Paths.get(directory,file));
    Map<String,String> map = new HashMap<>();
    for(String line : lines) {
      String parts[] = line.split("\t");
      map.put(parts[1], parts[0]);
    }
    return map;
  }

}
