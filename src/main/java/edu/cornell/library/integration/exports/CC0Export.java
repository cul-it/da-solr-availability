package edu.cornell.library.integration.exports;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import edu.cornell.library.integration.folio.DownloadMARC;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class CC0Export {


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
        prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass")); ){

      Set<String> bibs = ExportUtils.getBibsToExport(inventory);
      System.out.println("Bib count: "+bibs.size());
      int fileCount = 0;
      String filenamePattern = "cornell-export-300k-rec-chunks-%02d.xml";
      int recordsPerFile = 300_000;
      int recordsThisFile = 0;
      Path outputFile = Paths.get(String.format(filenamePattern,++fileCount));
      BufferedWriter writer = Files.newBufferedWriter(outputFile);
      writer.write("<?xml version='1.0' encoding='UTF-8'?>"
          + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

      BIB: for(String bibid : bibs) {

        Map<String,Object> instance = ExportUtils.getInstance( inventory, bibid);

        // if the bib(instance) is suppressed, we don't want to export.
        if ((instance.containsKey("discoverySuppress") && (boolean)instance.get("discoverySuppress"))
            || (instance.containsKey("staffSuppress") && (boolean)instance.get("staffSuppress")) ) {
          System.out.printf("Skipping %8s: bib suppressed\n",bibid);
          continue BIB;
        }

        MarcRecord bibRec = DownloadMARC.getMarc(
            inventory,MarcRecord.RecordType.BIBLIOGRAPHIC,bibid);

        if (bibRec == null) {
          System.out.printf("Skipping %8s: non-MARC instance\n",bibid);
          continue BIB;
        }

        // confirm the record isn't NoEx.
        for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
          for (Subfield sf : f.subfields) if (sf.value.contains("NoEx")) {
            System.out.printf("Skipping %8s: NoEx\n",bibid);
            continue BIB;
          }

        ExportUtils.cleanUnwantedDataFields(bibRec, null, Arrays.asList(new ExportUtils.FieldRange("857","999")),false);
        writer.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
            .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
        if ( ++recordsThisFile == recordsPerFile ) {
//        if ( Files.size(outputFile) > 209715200L /*200MB*/ ) {
          writer.write("</collection>\n");
          writer.flush();
          writer.close();
          outputFile = Paths.get(String.format(filenamePattern,++fileCount));
          writer = Files.newBufferedWriter(outputFile);
          writer.write("<?xml version='1.0' encoding='UTF-8'?>"
              + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
          recordsThisFile = 0;
        }
      }
      writer.write("</collection>\n");
      writer.flush();
      writer.close();

    }

  }

}
