package edu.cornell.library.integration.changes;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Locations;
import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.Locations.Location;
import edu.cornell.library.integration.folio.Locations.Sort;

public class LocationMonitor {

  public static void main(String[] args) throws IOException, SQLException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    StringBuilder log = new StringBuilder();
    try (
        Connection inventoryDB = DriverManager.getConnection(
            prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement selectAllStmt = inventoryDB.prepareStatement(
            "SELECT * FROM locationFolio");
        PreparedStatement updateStmt = inventoryDB.prepareStatement(
            "UPDATE locationFolio SET json = ? WHERE code = ?");
        PreparedStatement insertStmt = inventoryDB.prepareStatement(
            "INSERT INTO locationFolio (code, json) VALUES (?,?)");
        PreparedStatement deleteStmt = inventoryDB.prepareStatement(
            "DELETE FROM locationFolio WHERE code = ?");
        ){

      OkapiClient okapi = new OkapiClient(
          prop.getProperty("okapiUrlFolio"),
          prop.getProperty("okapiTokenFolio"),
          prop.getProperty("okapiTenantFolio"));

      Map<String,String> cachedLocations = new HashMap<>();
      try (ResultSet rs = selectAllStmt.executeQuery()) {
        while (rs.next()) cachedLocations.put(rs.getString("code"), rs.getString("json"));
      }

      new Locations(okapi);
      for (Location l : Locations.allLocations(Sort.CODE)) {
        String json = mapper.writeValueAsString(l);
        if ( cachedLocations.containsKey(l.code) ) {
          if ( cachedLocations.get(l.code).equals(json) ) {
            //location unchanged, remove from unmatched locations map
            cachedLocations.remove(l.code);
          } else {
            //location changed
            log.append("Location changed: "+cachedLocations.get(l.code)+" ==>> "+json+"\n");
            updateStmt.setString(1, json);
            updateStmt.setString(2, l.code);
            updateStmt.executeUpdate();
            cachedLocations.remove(l.code);
          }
        } else {
          // location added
          log.append("Location added: "+json+"\n");
          insertStmt.setString(1, l.code);
          insertStmt.setString(2, json);
          insertStmt.executeUpdate();
        }
      }

      // Locations remaining in cachedLocations were deleted
      for (String code : cachedLocations.keySet()) {
        log.append("Location deleted: "+cachedLocations.get(code)+"\n");
        deleteStmt.setString(1, code);
        deleteStmt.executeUpdate();
      }
    }
    if ( log.length() > 0 ) {
      System.out.println(log.toString());
      System.exit(1);
    }
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

}
