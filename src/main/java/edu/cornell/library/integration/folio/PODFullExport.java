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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

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
        prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass"));
        PreparedStatement insertBibPodStmt = inventory.prepareStatement(
         "INSERT INTO bibPod( instanceHrid, moddate, instanceModdate, podActive) VALUES (?,?,?,?)");
        PreparedStatement insertHoldingPodStmt = inventory.prepareStatement(
         "INSERT INTO holdingPod(hrid, instanceHrid, moddate, podActive, content) VALUES (?,?,?,?,?)");
        PreparedStatement insertItemPodStmt = inventory.prepareStatement(
         "INSERT INTO itemPod(hrid, holdingHrid, moddate, podActive) VALUES (?,?,?,?)");  ){

      OkapiClient okapi = new OkapiClient(
          prop.getProperty("okapiUrlFolio"),prop.getProperty("okapiTokenFolio"),
          prop.getProperty("okapiTenantFolio"));

      Locations locations = new Locations(okapi);
      ReferenceData holdingsNoteTypes = new ReferenceData(okapi, "/holdings-note-types","name");
      ReferenceData callNumberTypes = new ReferenceData(okapi, "/call-number-types","name");
      Items.initialize(okapi,locations);
      ServicePoints.initialize(okapi);
      LoanTypes.initialize(okapi);


      Set<String> bibs = getBibsToExport(inventory);
      System.out.println("Unsuppressed bib count: "+bibs.size());

      int fileNum = 1;
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(
          String.format("cornell-full-%02d.xml", fileNum)));
      int recordsInFile = 0;
      writer.write("<?xml version='1.0' encoding='UTF-8'?>"
          + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

      for (String bibId : bibs) {
        boolean sendToPod = true;
        Map<String,Object> instance = getInstance( inventory, bibId);
        Map<String,String> instanceMetadata = (Map<String,String>)instance.get("metadata");
        Timestamp instanceModdate = Timestamp.from(
            isoDT.parse(instanceMetadata.get("updatedDate"),Instant::from));
        MarcRecord bibRec = DownloadMARC.getMarc(inventory,MarcRecord.RecordType.BIBLIOGRAPHIC,bibId);
        if ( bibRec == null ) {
          System.out.printf("Skipping non-MARC instance #%s\n",bibId);
          insertBibPodStmt.setString(1,(String)instance.get("hrid"));
          insertBibPodStmt.setTimestamp(2, null);
          insertBibPodStmt.setTimestamp(3, instanceModdate);
          insertBibPodStmt.setBoolean(4, false);
          insertBibPodStmt.executeUpdate();
          continue;
        }

        // if the bib(instance) is suppressed, we don't want to send to POD.
        if ((instance.containsKey("discoverySuppress") && (boolean)instance.get("discoverySuppress"))
            || (instance.containsKey("staffSuppress") && (boolean)instance.get("staffSuppress")) )
          sendToPod = false;

        // if not suppressed, confirm the record isn't NoEx.
        else for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
          for (Subfield sf : f.subfields) if (sf.value.contains("NoEx"))
            sendToPod = false;

        boolean unsuppressedPrintHoldings =
            addHoldingsDataToBibRec(bibRec,inventory,locations,holdingsNoteTypes,callNumberTypes, insertHoldingPodStmt, insertItemPodStmt);

        // If there are no unsuppressed print holdings, the record is also not eligible for POD
        if ( ! unsuppressedPrintHoldings )
          sendToPod = false;

        if ( ! sendToPod ) {
          insertBibPodStmt.setString(1,(String)instance.get("hrid"));
          insertBibPodStmt.setTimestamp(2, bibRec.moddate);
          insertBibPodStmt.setTimestamp(3, instanceModdate);
          insertBibPodStmt.setBoolean(4, false);
          insertBibPodStmt.executeUpdate();
          continue;
        }

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
        writer.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
            .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
        insertBibPodStmt.setString(1,(String)instance.get("hrid"));
        insertBibPodStmt.setTimestamp(2, bibRec.moddate);
        insertBibPodStmt.setTimestamp(3, instanceModdate);
        insertBibPodStmt.setBoolean(4, true);
        insertBibPodStmt.executeUpdate();
        recordsInFile++;
      }
      if ( recordsInFile > 0 ) {
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
        System.out.printf("Closing file %02d\n", fileNum);
      }
    }
  }

  private static boolean addHoldingsDataToBibRec(
      MarcRecord bibRec, Connection inventory, Locations locations,
      ReferenceData holdingsNoteTypes, ReferenceData callNumberTypes,
      PreparedStatement insertHoldingPodStmt, PreparedStatement insertItemPodStmt)
          throws SQLException, IOException {
    int maxBibFieldId = bibRec.dataFields.last().id;
    HoldingSet holdings = Holdings.retrieveHoldingsByInstanceHrid(
        inventory, locations, holdingsNoteTypes, callNumberTypes, bibRec.id);
    ItemList items = Items.retrieveItemsForHoldings(null, inventory, bibRec.id, holdings);

    boolean unsuppressedPrintHoldings = false;
    for ( String holdingUuid : holdings.getUuids()) {
      Holding h = holdings.get(holdingUuid);
      if ( h.active == false ) continue;
      if ( h.online != null && h.online == true ) continue;
      unsuppressedPrintHoldings = true;
      MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
      DataField f852 = new DataField( ++maxBibFieldId, "852" );
      int sfId = 0;
      f852.subfields.add(new Subfield(++sfId,'0',h.hrid));
      if ( h.location != null ) {
        if ( h.location.library != null )
          f852.subfields.add(new Subfield(++sfId,'a',h.location.library));
        if ( h.location.name != null)
          f852.subfields.add(new Subfield(++sfId,'b',h.location.name));
      }
      if ( h.rawFolioHolding != null ) {
        Map<String,Object> raw = h.rawFolioHolding;

        if ( raw.containsKey("callNumberPrefix") ) {
          String prefix = (((String)raw.get("callNumberPrefix")).replaceAll("\n", "")).trim();
          if ( ! prefix.isEmpty() ) f852.subfields.add(new Subfield(++sfId,'k',prefix));
        }

        if ( raw.containsKey("callNumber") ) {
          String callNum = (((String)raw.get("callNumber")).replaceAll("\n", "")).trim();
          if ( ! callNum.isEmpty() ) f852.subfields.add(new Subfield(++sfId,'h', callNum));
        }

        if ( raw.containsKey("callNumberSuffix") ) {
          String suffix = (((String)raw.get("callNumberSuffix")).replaceAll("\n", "")).trim();
          if (! suffix.isEmpty() ) f852.subfields.add(new Subfield(++sfId,'m',suffix));
        }

        if ( raw.containsKey("callNumberTypeId") ) {
          String type = callNumberTypes.getName((String)raw.get("callNumberTypeId"));
          switch (type) {
          case "Library of Congress classification":
          case "LC Modified":
            f852.subfields.add(new Subfield(++sfId,'2',"lc"));
            f852.ind1 = '0';
            break;
          case "Dewey Decimal classification":
            f852.subfields.add(new Subfield(++sfId,'2',"dewey"));
            f852.ind1 = '1';
            break;
          default: f852.ind1 = '8';
          }
        }

        if (raw.containsKey("copyNumber")) {
          String copyNo = (((String)raw.get("copyNumber")).replaceAll("\n", "")).trim();
          if ( ! copyNo.isEmpty() ) f852.subfields.add(new Subfield(++sfId,'t',copyNo));
        }
      }
      holdingRec.dataFields.add(f852);

      if ( h.rawFolioHolding != null ) {
        Map<String,Object> raw = h.rawFolioHolding;

        if ( raw.containsKey("holdingsStatements") ) {
          List<Map<String,String>> a = (List<Map<String,String>>)raw.get("holdingsStatements");
          for (Map<String,String> statement : a) {
            if ( statement == null ) continue;
            if ( ! statement.containsKey("statement") ) continue;
            DataField f866 = new DataField ( ++maxBibFieldId, "866" );
            f866.subfields.add(new Subfield(1,'0',h.hrid));
            f866.subfields.add(new Subfield(2,'a',
                statement.get("statement").replaceAll("\n", "")));
            if ( statement.containsKey("note") )
              f866.subfields.add(new Subfield(3,'z',
                  statement.get("note").replaceAll("\n", "")));
            holdingRec.dataFields.add(f866);
          }
        }

        if ( raw.containsKey("holdingsStatementsForSupplements") ) {
          List<Map<String,String>> a =
              (List<Map<String,String>>)raw.get("holdingsStatementsForSupplements");
          for (Map<String,String> statement : a) {
            if ( statement == null ) continue;
            if ( ! statement.containsKey("statement") ) continue;
            DataField f867 = new DataField ( ++maxBibFieldId, "867" );
            f867.subfields.add(new Subfield(1,'0',h.hrid));
            f867.subfields.add(new Subfield(2,'a',
                statement.get("statement").replaceAll("\n", "")));
            if ( statement.containsKey("note") )
              f867.subfields.add(new Subfield(3,'z',
                  statement.get("note").replaceAll("\n", "")));
            holdingRec.dataFields.add(f867);
          }
        }

        if ( raw.containsKey("holdingsStatementsForIndexes") ) {
          List<Map<String,String>> a =
              (List<Map<String,String>>)raw.get("holdingsStatementsForIndexes");
          for (Map<String,String> statement : a) {
            if ( statement == null ) continue;
            if ( ! statement.containsKey("statement") ) continue;
            DataField f868 = new DataField ( ++maxBibFieldId, "868" );
            f868.subfields.add(new Subfield(1,'0',h.hrid));
            f868.subfields.add(new Subfield(2,'a',
                statement.get("statement").replaceAll("\n", "")));
            if ( statement.containsKey("note") )
              f868.subfields.add(new Subfield(3,'z',
                  statement.get("note").replaceAll("\n", "")));
            holdingRec.dataFields.add(f868);
          }
        }

        for ( Item item : items.getItems().get(holdingUuid) ) {
          if ( ! item.active ) continue;
          DataField f890 = new DataField(++maxBibFieldId, "890");
          f890.subfields.add(new Subfield(1,'0',h.hrid));
          f890.subfields.add(new Subfield(2,'a',item.id));
          f890.subfields.add(new Subfield(3,'t',item.matType.get("name")));
          f890.subfields.add(new Subfield(4,'l',item.location.code));
          if ( item.empty == null || ! item.empty )
            f890.subfields.add(new Subfield(5,'b',item.barcode));
          if ( item.enumeration != null && ! item.enumeration.isEmpty() )
            f890.subfields.add(new Subfield(6,'e',item.enumeration));
          if ( item.chron != null && ! item.chron.isEmpty() )
            f890.subfields.add(new Subfield(7,'c',item.chron));
          if ( item.rawFolioItem.containsKey("yearCaption") ) {
            String yearCaption = ((String)item.rawFolioItem.get("yearCaption")).trim();
            if ( ! yearCaption.isEmpty() ) f890.subfields.add(new Subfield(8,'y',yearCaption));
          }
          if ( item.rawFolioItem.containsKey("numberOfPieces") ) {
            Object pieces = item.rawFolioItem.get("numberOfPieces");
            if ( pieces != null ) {
              if (pieces instanceof Integer) {
                f890.subfields.add(new Subfield(9,'n',String.valueOf(pieces)));
              } else if (pieces instanceof String){
                if ( ! ((String)pieces).isEmpty() )
                  f890.subfields.add(new Subfield(9,'n',(String)pieces));
              } else {
                System.out.printf("Unrecognized class of pieces %s (%s/%s/%s)\n",
                    pieces.getClass().getName(),bibRec.id,h.hrid,item.hrid);
              }
            }
          }
          holdingRec.dataFields.add(f890);
          insertItemPodStmt.setString(1, item.hrid);
          insertItemPodStmt.setString(2, h.hrid);
          Map<String,String> itemMetadata = (Map<String,String>)item.rawFolioItem.get("metadata");
          Timestamp itemModdate = ( itemMetadata == null )
              ? Timestamp.valueOf("2020-07-01 00:00:00")
              : Timestamp.from(isoDT.parse(itemMetadata.get("updatedDate"),Instant::from));
          insertItemPodStmt.setTimestamp(3, itemModdate);
          insertItemPodStmt.setBoolean(4, true);
          insertItemPodStmt.addBatch();
        }
        insertItemPodStmt.executeBatch();

      }
      insertHoldingPodStmt.setString(1, h.hrid);
      insertHoldingPodStmt.setString(2, bibRec.id);
      Map<String,String> holdingMetadata = (Map<String,String>)h.rawFolioHolding.get("metadata");
      Timestamp holdingModdate = Timestamp.from(
          isoDT.parse(holdingMetadata.get("updatedDate"),Instant::from));
      insertHoldingPodStmt.setTimestamp(3, holdingModdate);
      insertHoldingPodStmt.setBoolean(4, true);
      insertHoldingPodStmt.setString(5,holdingRec.toString());
      insertHoldingPodStmt.executeUpdate();
      bibRec.dataFields.addAll(holdingRec.dataFields);
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
          "SELECT instanceHrid FROM bibFolio")) {
        while ( rs.next() ) bibs.add(rs.getString(1));
      }
    }
    return bibs;
  }

  private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("Z"));
  private static ObjectMapper mapper = new ObjectMapper();

}
