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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class PODFullExport {

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("database.properties")){ prop.load(in); }

    try (Connection inventory = DriverManager.getConnection(prop.getProperty("inventoryDBUrl"),
        prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"))){

      OkapiClient okapi = new OkapiClient(
          prop.getProperty("okapiUrlFolio"),prop.getProperty("okapiTokenFolio"),
          prop.getProperty("okapiTenantFolio"));

      Locations locations = new Locations(okapi);
      ReferenceData holdingsNoteTypes = new ReferenceData(okapi, "/holdings-note-types","name");
      ReferenceData callNumberTypes = new ReferenceData(okapi, "/call-number-types","name");

      Set<String> bibs = getBibsToExport(inventory);
      System.out.println("Unsuppressed bib count: "+bibs.size());

      int fileNum = 1;
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(
          String.format("cornell-full-%03d.xml", fileNum)));
      int recordsInFile = 0;
      writer.write("<?xml version='1.0' encoding='UTF-8'?>"
          + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

      for (String bibId : bibs) {
        boolean sendToPod = true;
        MarcRecord bibRec = DownloadMARC.getMarc(inventory,MarcRecord.RecordType.BIBLIOGRAPHIC,bibId);
        if ( bibRec == null ) {
          System.out.printf("Skipping non-MARC instance #%s\n",bibId);
          continue;
        }
        Map<String,Object> instance = getInstance( inventory, bibId);

        // if the bib(instance) is suppressed, we don't want to send to POD.
        if ( instance != null &&
            ((instance.containsKey("discoverySuppress") && (boolean)instance.get("discoverySuppress"))
            || (instance.containsKey("staffSuppress") && (boolean)instance.get("staffSuppress"))))
          sendToPod = false;

        // if not suppressed, confirm the record isn't NoEx.
        else for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
          for (Subfield sf : f.subfields) if (sf.value.contains("NoEx"))
            sendToPod = false;

        boolean unsuppressedPrintHoldings =
            addHoldingsDataToBibRec(bibRec,inventory,locations,holdingsNoteTypes,callNumberTypes);

        // If there are no unsuppressed print holdings, the record is also not eligible for POD
        if ( ! unsuppressedPrintHoldings )
          sendToPod = false;

        if ( ! sendToPod ) continue;

        if ( recordsInFile >= 50_000 ) {
          writer.write("</collection>\n");
          writer.flush();
          writer.close();
          System.out.printf("Closing file %03d\n", fileNum);
          writer = new BufferedWriter(Files.newBufferedWriter(
              Paths.get(String.format("cornell-full-%03d.xml",++fileNum))));
          recordsInFile = 0;
          writer.write( "<?xml version='1.0' encoding='UTF-8'?>"
              + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
        }
        writer.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
            .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
        recordsInFile++;
      }
      if ( recordsInFile > 0 ) {
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
        System.out.printf("Closing file %03d\n", fileNum);
      }
    }
  }

  private static boolean addHoldingsDataToBibRec(
      MarcRecord bibRec, Connection inventory, Locations locations,
      ReferenceData holdingsNoteTypes, ReferenceData callNumberTypes)
          throws SQLException, IOException {
    int maxBibFieldId = bibRec.dataFields.last().id;
    HoldingSet holdings = Holdings.retrieveHoldingsByInstanceHrid(
        inventory, locations, holdingsNoteTypes, callNumberTypes, bibRec.id);
    boolean unsuppressedPrintHoldings = false;
    for ( Holding h : holdings.values() ) {
      if ( h.active == false ) continue;
      if ( h.online != null && h.online == true ) continue;
      unsuppressedPrintHoldings = true;
      DataField f852 = new DataField( ++maxBibFieldId, "852" );
      int sfId = 0;
      if ( h.location != null ) {
        if ( h.location.library != null )
          f852.subfields.add(new Subfield(++sfId,'a',h.location.library));
        if ( h.location.name != null)
          f852.subfields.add(new Subfield(++sfId,'b',h.location.name));
      }
      if ( h.rawFolioHolding != null ) {
        Map<String,Object> raw = h.rawFolioHolding;

        if ( raw.containsKey("callNumberPrefix") ) f852.subfields.add(new Subfield(++sfId,'k',
              ((String)raw.get("callNumberPrefix")).replaceAll("\n", "")));

        if ( raw.containsKey("callNumber") ) f852.subfields.add(new Subfield(++sfId,'h',
              ((String)raw.get("callNumber")).replaceAll("\n", "")));

        if ( raw.containsKey("callNumberSuffix") )
          f852.subfields.add(new Subfield(++sfId,'m',
              ((String)raw.get("callNumberSuffix")).replaceAll("\n", "")));

        if ( raw.containsKey("callNumberTypeId") ) {
          String type = callNumberTypes.getName((String)raw.get("callNumberTypeId"));
          switch (type) {
          case "Library of Congress classification": f852.ind1 = '0'; break;
          case "LC Modified": f852.ind1 = '0'; break;
          case "Dewey Decimal classification": f852.ind1 = '1'; break;
          default: f852.ind1 = '8';
          }
        }

        if (raw.containsKey("copyNumber")) 
          f852.subfields.add(new Subfield(++sfId,'t',
              ((String)raw.get("copyNumber")).replaceAll("\n", "")));
      }
      bibRec.dataFields.add(f852);

      if ( h.rawFolioHolding != null ) {
        Map<String,Object> raw = h.rawFolioHolding;

        if ( raw.containsKey("holdingsStatements") ) {
          List<Map<String,String>> a = (List<Map<String,String>>)raw.get("holdingsStatements");
          for (Map<String,String> statement : a) {
            if ( statement == null ) continue;
            if ( ! statement.containsKey("statement") ) continue;
            DataField f866 = new DataField ( ++maxBibFieldId, "866" );
            f866.subfields.add(new Subfield(1,'a',
                statement.get("statement").replaceAll("\n", "")));
            if ( statement.containsKey("note") )
              f866.subfields.add(new Subfield(2,'z',
                  statement.get("note").replaceAll("\n", "")));
            bibRec.dataFields.add(f866);
          }
        }

        if ( raw.containsKey("holdingsStatementsForSupplements") ) {
          List<Map<String,String>> a =
              (List<Map<String,String>>)raw.get("holdingsStatementsForSupplements");
          for (Map<String,String> statement : a) {
            if ( statement == null ) continue;
            if ( ! statement.containsKey("statement") ) continue;
            DataField f867 = new DataField ( ++maxBibFieldId, "867" );
            f867.subfields.add(new Subfield(1,'a',
                statement.get("statement").replaceAll("\n", "")));
            if ( statement.containsKey("note") )
              f867.subfields.add(new Subfield(2,'z',
                  statement.get("note").replaceAll("\n", "")));
            bibRec.dataFields.add(f867);
          }
        }

        if ( raw.containsKey("holdingsStatementsForIndexes") ) {
          List<Map<String,String>> a =
              (List<Map<String,String>>)raw.get("holdingsStatementsForIndexes");
          for (Map<String,String> statement : a) {
            if ( statement == null ) continue;
            if ( ! statement.containsKey("statement") ) continue;
            DataField f868 = new DataField ( ++maxBibFieldId, "868" );
            f868.subfields.add(new Subfield(1,'a',
                statement.get("statement").replaceAll("\n", "")));
            if ( statement.containsKey("note") )
              f868.subfields.add(new Subfield(2,'z',
                  statement.get("note").replaceAll("\n", "")));
            bibRec.dataFields.add(f868);
          }
        }

      }
    }
    return unsuppressedPrintHoldings;
  }

  private static Map<String, Object> getInstance(Connection inventory, String bibId)
      throws SQLException, IOException {
    try ( PreparedStatement instanceByHrid = inventory.prepareStatement
            ("SELECT * FROM instanceFolio WHERE hrid = ?") ) {
      instanceByHrid.setString(1, bibId);
      try ( ResultSet rs1 = instanceByHrid.executeQuery() ) {
        while (rs1.next()) return mapper.readValue( rs1.getString("content"), Map.class);
      }
    }
    return null;
  }

  private static Set<String> getBibsToExport(Connection inventory) throws SQLException {
    Set<String> bibs = new TreeSet<>();
    try ( Statement stmt = inventory.createStatement() ){
      stmt.setFetchSize(1_000_000);
      try ( ResultSet rs = stmt.executeQuery(
          "SELECT instanceHrid FROM bibFolio WHERE podCurrent = 0")) {
        while ( rs.next() ) bibs.add(rs.getString(1));
      }
      try ( ResultSet rs = stmt.executeQuery(
          "SELECT instanceHrid FROM holdingFolio WHERE podCurrent = 0")) {
        while ( rs.next() ) bibs.add(rs.getString(1));
      }
    }
    return bibs;
  }

  private static ObjectMapper mapper = new ObjectMapper();

}
