package edu.cornell.library.integration.voyager;

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
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;

public class PODFullExport {

  public static void main(String[] args) throws IOException, SQLException, XMLStreamException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    try (Connection voyager = DriverManager.getConnection(
         prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
         Connection inventoryDB = DriverManager.getConnection(
         prop.getProperty("inventoryDBUrl"),prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"))){
      Locations locations = new Locations(voyager);

      Set<Integer> bibs = getUnsuppressedBibs(voyager);
      System.out.println("Unsuppressed bib count: "+bibs.size());
      int noexBibsSkipped = 0, onlineBibsSkipped = 0;

      int fileNum = 1;
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(String.format("cornell-full-%03d.xml", fileNum)));
      int recordsInFile = 0;
      writer.write(
          "<?xml version='1.0' encoding='UTF-8'?><collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

      bib: for (Integer bibId : bibs) {
        MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC,
            DownloadMARC.downloadMrc(voyager, MarcRecord.RecordType.BIBLIOGRAPHIC,  bibId));

        for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
          for (Subfield sf : f.subfields) if (sf.value.contains("NoEx")) {
            noexBibsSkipped++;
            System.out.println("Bib "+bibId+" is NoEx. Skipping.");
            continue bib;
          }

        int maxBibFieldId = bibRec.dataFields.last().id;
        HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyager, bibId);
        int unsuppressedPrintHoldingsCount = 0;
        for ( Holding h : holdings.values() ) {
          if ( h.active == false ) continue;
          if ( h.location == null || h.location.equals(locations.getByCode("serv,remo")) ) continue;
          unsuppressedPrintHoldingsCount++;
          DataField f852 = null;
          for ( DataField f : h.record.dataFields ) if ( f.tag.equals("852") )
              f852 = f;

          if (f852 != null) {
            Subfield f852b = null;
            for ( Subfield sf : f852.subfields ) if ( sf.code.equals('b') )
              f852b = sf;
            if ( f852b != null && h.location != null) {
              f852b.value = h.location.name;
              if ( h.location.library != null )
                f852.subfields.add(new Subfield (-1, 'a', h.location.library ));
            }
            f852.id = ++maxBibFieldId;
            bibRec.dataFields.add(f852);
          }
          for ( DataField f : h.record.dataFields ) if (f.tag.startsWith("86") ) {
            DataField newF = new DataField(++maxBibFieldId,f.tag);
            newF.ind1 = f.ind1;
            newF.ind2 = f.ind2;
            newF.subfields = f.subfields;
            bibRec.dataFields.add(newF);
          }
        }
        if ( unsuppressedPrintHoldingsCount == 0 ) {
          onlineBibsSkipped++;
          System.out.println("Bib "+bibId+" is online. Skipping.");
          continue bib;
        }

        if ( recordsInFile >= 50_000 ) {
          writer.write("</collection>\n");
          writer.flush();
          writer.close();
          System.out.printf("Closing file %03d with %d skipped NoEx and %d skipped online\n",
              fileNum,noexBibsSkipped,onlineBibsSkipped);
          writer = new BufferedWriter(Files.newBufferedWriter(Paths.get(String.format("cornell-full-%03d.xml",++fileNum))));
          recordsInFile = 0;
          writer.write(
              "<?xml version='1.0' encoding='UTF-8'?><collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
        }
        writer.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
            .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
        recordsInFile++;
      }
      if ( recordsInFile > 0 ) {
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
        System.out.printf("Closing file %03d with %d skipped NoEx and %d skipped online\n",
            fileNum,noexBibsSkipped,onlineBibsSkipped);
      }
    }
  }

  private static Set<Integer> getUnsuppressedBibs(Connection voyager) throws SQLException {
    Set<Integer> bibs = new TreeSet<>();
    try ( Statement stmt = voyager.createStatement() ){
      stmt.setFetchSize(1_000_000);
      try ( ResultSet rs = stmt.executeQuery("SELECT bib_id FROM bib_master WHERE suppress_in_opac = 'N'")) {
        while ( rs.next() ) bibs.add(rs.getInt(1));
      }
    }
    return bibs;
  }

}
