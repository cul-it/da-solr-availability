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
public class PopulateBarcodesToDBForPreLoadItems {

  public static void main(String[] args) throws IOException, SQLException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("database.properties")){
      prop.load(in);
    }

    try (Connection inventory = DriverManager.getConnection(
        prop.getProperty("inventoryDBUrl"),
        prop.getProperty("inventoryDBUser"),
        prop.getProperty("inventoryDBPass"));
        PreparedStatement q = inventory.prepareStatement(
            "SELECT * FROM itemFolio"
            + " WHERE barcode IS NULL AND content LIKE '%\"barcode\"%' LIMIT 100");
        PreparedStatement u = inventory.prepareStatement(
            "UPDATE itemFolio SET barcode = ? WHERE id = ?")) {
      int foundCount;
      do {
        foundCount = 0;
        try ( ResultSet rs = q.executeQuery() ) {
          while ( rs.next() ) {
            foundCount++;
            Matcher m = barcodeP.matcher(rs.getString("content"));
            if ( ! m.matches() ) {
              System.out.println("No barcode: "+rs.getString("content"));
              continue;
            }
            String barcode = m.group(1);
            if (barcode.length() > 15) continue;
            u.setString(1, barcode);
            u.setString(2, rs.getString("id"));
            u.addBatch();
          }
          u.executeBatch();
        }
      } while ( foundCount == 100 );
      
    }
  }
  static Pattern barcodeP = Pattern.compile("^.*\"barcode\": *\"([^\"]+)\".*$");

}
