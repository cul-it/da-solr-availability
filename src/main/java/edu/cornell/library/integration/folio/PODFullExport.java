package edu.cornell.library.integration.folio;

import java.io.BufferedWriter;
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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class PODFullExport {

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("database.properties")){ prop.load(in); }

    try (Connection inventory = DriverManager.getConnection(prop.getProperty("inventoryDBUrl"),
        prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass")); ){

      OkapiClient okapi = new OkapiClient(
          prop.getProperty("okapiUrlFolio"),prop.getProperty("okapiTokenFolio"),
          prop.getProperty("okapiTenantFolio"));

      PODExporter exporter = new PODExporter( inventory, okapi );

      Set<String> bibs = getBibsToExport(inventory);
      System.out.println("Bib count: "+bibs.size());

      int fileNum = 1;
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(
          String.format("cornell-full-%02d.xml", fileNum)));
      int recordsInFile = 0;
      writer.write("<?xml version='1.0' encoding='UTF-8'?>"
          + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

      for (String bibId : bibs) {

        if ( recordsInFile >= 500_000 ) {
          writer.write("</collection>\n");
          writer.flush();
          writer.close();
          System.out.printf("Closing file %02d\n", fileNum);
          writer = new BufferedWriter(Files.newBufferedWriter(
              Paths.get(String.format("cornell-full-%02d.xml",++fileNum))));
          recordsInFile = 0;
          writer.write( "<?xml version='1.0' encoding='UTF-8'?>"
              + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
        }

        if  (exporter.exportBib( bibId,writer,null) ) {
          recordsInFile++;
        }

      }
      if ( recordsInFile > 0 ) {
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
        System.out.printf("Closing file %02d\n", fileNum);
      }
    }
  }

  private static Set<String> getBibsToExport(Connection inventory) throws SQLException {
    Set<String> bibs = new LinkedHashSet<>();
    try ( Statement stmt = inventory.createStatement() ){
      stmt.setFetchSize(1_000_000);
      try ( ResultSet rs = stmt.executeQuery(
          "SELECT instanceHrid FROM bibFolio ORDER BY RAND()")) {
        while ( rs.next() ) bibs.add(rs.getString(1));
      }
    }
    return bibs;
  }

}
