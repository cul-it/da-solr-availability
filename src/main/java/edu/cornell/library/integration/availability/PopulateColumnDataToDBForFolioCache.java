package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** One-off data remediation method */
public class PopulateColumnDataToDBForFolioCache {

  public static void main(String[] args) throws IOException, SQLException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("database.properties")){
      prop.load(in);
    }

    try (Connection inventory = DriverManager.getConnection(
        prop.getProperty("databaseURLCurrent"),
        prop.getProperty("databaseUserCurrent"),
        prop.getProperty("databasePassCurrent"));
        PreparedStatement q = inventory.prepareStatement(
            "SELECT * FROM instanceFolio WHERE source IS NULL LIMIT 1000");
        PreparedStatement u = inventory.prepareStatement(
            "UPDATE instanceFolio SET source = ? WHERE id = ?")) {
      int foundCount;
      do {
        foundCount = 0;
        try ( ResultSet rs = q.executeQuery() ) {
          while ( rs.next() ) {
            foundCount++;
            Matcher m = sourceP.matcher(rs.getString("content"));
            if ( ! m.matches() ) {
              System.out.println("No source: "+rs.getString("content"));
              continue;
            }
            String source = m.group(1);
            if (source.length() > 12) continue;
            u.setString(1, source);
            u.setString(2, rs.getString("id"));
            u.addBatch();
          }
          u.executeBatch();
        }
      } while ( foundCount == 1000 );
      
    }
  }
  static Pattern sourceP = Pattern.compile("^.*\"source\": *\"([^\"]+)\".*$");

}
