package edu.cornell.library.integration.folio;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import edu.cornell.library.integration.folio.PODExporter.UpdateType;

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


      Calendar cal = Calendar.getInstance();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      String today = sdf.format(cal.getTime());
      try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(
          String.format("cornell-full-%s.xml", today)))) {

        writer.write("<?xml version='1.0' encoding='UTF-8'?>"
            + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
        int recordsExported = 0;
        int recordsProcessed = 0;

        for (String bibId : bibs) {

          recordsProcessed++;
          if  (exporter.exportBib( bibId,writer,null).equals(UpdateType.UPDATE) )
            recordsExported++;
          if ( recordsProcessed % 100_000 == 0 )
            System.out.printf("%d of %d records: (%s)\n",recordsExported,recordsProcessed,bibId);

        }
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
        System.out.printf("%d of %d total records exported\n",recordsExported,recordsProcessed);
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
