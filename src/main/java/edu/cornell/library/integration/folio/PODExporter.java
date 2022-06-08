package edu.cornell.library.integration.folio;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class PODExporter {

  private final Locations locations;
  private final ReferenceData holdingsNoteTypes;
  private final ReferenceData callNumberTypes;

  private final Connection inventory;
  private final PreparedStatement insertBibPodStmt;
  private final PreparedStatement insertHoldingPodStmt;
  private final PreparedStatement insertItemPodStmt;
  private final PreparedStatement getPreviousBibStatusStmt;
  private final PreparedStatement getHoldingPodStmt;
  private final PreparedStatement getItemPodStmt;
  private final PreparedStatement getHoldingFolioStmt;
  private final PreparedStatement getItemFolioStmt;
  private final PreparedStatement deleteHoldingPodStmt;
  private final PreparedStatement deleteItemPodStmt;

  public PODExporter ( Connection inventory, OkapiClient okapi ) throws IOException, SQLException {
    
    this.locations = new Locations(okapi);
    this.holdingsNoteTypes = new ReferenceData(okapi, "/holdings-note-types","name");
    this.callNumberTypes = new ReferenceData(okapi, "/call-number-types","name");
    Items.initialize(okapi,this.locations);
    ServicePoints.initialize(okapi);
    LoanTypes.initialize(okapi);

    this.inventory = inventory;
    this.insertBibPodStmt = inventory.prepareStatement(
        "REPLACE INTO bibPod( instanceHrid, moddate, instanceModdate, podActive) VALUES (?,?,?,?)");
    this.insertHoldingPodStmt = inventory.prepareStatement(
        "INSERT INTO holdingPod(hrid, instanceHrid, moddate, podActive, content) VALUES (?,?,?,?,?)");
    this.insertItemPodStmt = inventory.prepareStatement(
        "INSERT INTO itemPod(hrid, holdingHrid, moddate, podActive) VALUES (?,?,?,?)");
    this.getHoldingPodStmt = inventory.prepareStatement(
        "SELECT * FROM holdingPod WHERE instanceHrid = ?");
    this.getPreviousBibStatusStmt = inventory.prepareStatement(
        "SELECT * FROM bibPod WHERE instanceHrid = ?");
    this.getItemPodStmt = inventory.prepareStatement(
        "SELECT * FROM itemPod WHERE holdingHrid = ?");
    this.getHoldingFolioStmt = inventory.prepareStatement(
        "SELECT * FROM holdingFolio WHERE instanceHrid = ?");
    this.getItemFolioStmt = inventory.prepareStatement(
        "SELECT * FROM itemFolio WHERE holdingHrid = ?");
    this.deleteHoldingPodStmt = inventory.prepareStatement(
        "DELETE FROM holdingPod WHERE instanceHrid = ? AND hrid = ?");
    this.deleteItemPodStmt = inventory.prepareStatement(
        "DELETE FROM itemPod WHERE holdingHrid = ? AND hrid = ?");
  }

  public boolean exportBib(
      String instanceHrid, BufferedWriter recordWriter, BufferedWriter deleteWriter)
          throws SQLException, IOException {

    PreviousBibStatus prevStatus = getPrevPodStatus(instanceHrid);

    Map<String,Object> instance = getInstance( this.inventory, instanceHrid);
    Map<String,String> instanceMetadata = (Map<String,String>)instance.get("metadata");
    Timestamp instanceModdate = Timestamp.from(
        isoDT.parse(instanceMetadata.get("updatedDate"),Instant::from));
    MarcRecord bibRec = DownloadMARC.getMarc(
        this.inventory,MarcRecord.RecordType.BIBLIOGRAPHIC,instanceHrid);
    if ( bibRec == null ) {
      System.out.printf("Skipping non-MARC instance #%s\n",instanceHrid);
      updatePodInventoryForInactiveInstance(instance,null,instanceModdate);
      if (prevStatus.active) deleteWriter.write(instanceHrid+'\n');
      return false;
    }

    // if the bib(instance) is suppressed, we don't want to send to POD.
    if ((instance.containsKey("discoverySuppress") && (boolean)instance.get("discoverySuppress"))
        || (instance.containsKey("staffSuppress") && (boolean)instance.get("staffSuppress")) ) {
      System.out.println(instanceHrid+" inactive due to suppression.");
      updatePodInventoryForInactiveInstance(instance,bibRec.moddate,instanceModdate);
      if (prevStatus.active) deleteWriter.write(instanceHrid+'\n');
      return false;
    }

    // if not suppressed, confirm the record isn't NoEx.
    for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
      for (Subfield sf : f.subfields) if (sf.value.contains("NoEx")) {
        System.out.println(instanceHrid+" inactive due NoEx.");
        updatePodInventoryForInactiveInstance(instance,bibRec.moddate,instanceModdate);
        if (prevStatus.active) deleteWriter.write(instanceHrid+'\n');
        return false;
      }

    HoldingsAndItems holdingsAndItems = collateHoldingsAndItemsData(bibRec);
    boolean podActiveHoldings = false;
    for (Holding h : holdingsAndItems.holdings.values())
      if ( h.active ) podActiveHoldings = true;

    if ( ! podActiveHoldings ) {
      System.out.println(instanceHrid+" inactive due to no active holdings.");
      updatePodInventoryForInactiveInstance(instance,bibRec.moddate,instanceModdate);
      if (prevStatus.active) deleteWriter.write(instanceHrid+'\n');
      return false;
    }

    if ( ! prevStatus.active
        || bibRec.moddate.after(prevStatus.moddate)
        || areActiveHoldingsChanged(instanceHrid,holdingsAndItems) ) {

      System.out.println("Writing "+instanceHrid+" to export.");
      updatePodInventoryForActiveInstance(instance,bibRec.moddate,instanceModdate,holdingsAndItems);
      recordWriter.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
          .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
      return true;
    }

    System.out.println(instanceHrid+" active but contains no substantive changes.");
    updatePodInventoryForActiveInstance(instance,bibRec.moddate,instanceModdate,holdingsAndItems);
    return false;//podActive, but no substantive change. Don't write to output
  }

  private boolean areActiveHoldingsChanged(String instanceHrid, HoldingsAndItems holdingsAndItems)
      throws SQLException {
    Map<String,String> newActiveHoldings = new HashMap<>();
    Map<String,String> oldActiveHoldings = new HashMap<>();
    for (String holdingUuid : holdingsAndItems.holdings.getUuids()) {
      Holding h = holdingsAndItems.holdings.get(holdingUuid);
      if ( h.active ) newActiveHoldings.put(h.hrid, h.marc.toString());
    }
    this.getHoldingPodStmt.setString(1, instanceHrid);
    try (ResultSet rs = this.getHoldingPodStmt.executeQuery()) {
      while ( rs.next() ) if ( rs.getBoolean("podActive") )
        oldActiveHoldings.put(rs.getString("hrid"), rs.getString("content"));
    }
    if (oldActiveHoldings.size() != newActiveHoldings.size()) return true;
    for ( String holdingHrid : oldActiveHoldings.keySet() ) {
      if ( ! newActiveHoldings.containsKey(holdingHrid) ) return true;
      if ( ! oldActiveHoldings.get(holdingHrid).equals(newActiveHoldings.get(holdingHrid)) )
        return true;
    }
    return false;
  }

  private class HoldingsAndItems {
    public HoldingSet holdings;
    public ItemList items;
  }
  private HoldingsAndItems collateHoldingsAndItemsData(MarcRecord bibRec)
      throws SQLException, IOException {
    int maxBibFieldId = bibRec.dataFields.last().id;
    HoldingsAndItems values = new HoldingsAndItems();
    values.holdings = Holdings.retrieveHoldingsByInstanceHrid(
        this.inventory, this.locations, this.holdingsNoteTypes, this.callNumberTypes, bibRec.id);
    values.items = Items.retrieveItemsForHoldings(null, this.inventory, bibRec.id, values.holdings);

    for ( String holdingUuid : values.holdings.getUuids()) {
      Holding h = values.holdings.get(holdingUuid);
      if ( h.active == false ) continue;
      if ( h.online != null && h.online == true ) { h.active = false; continue; }
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
          String type = this.callNumberTypes.getName((String)raw.get("callNumberTypeId"));
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
            if ( statement.containsKey("note") ) {
              String note = statement.get("note").replaceAll("\n", "");
              if ( ! note.isEmpty() ) f866.subfields.add(new Subfield(3,'z',note));
            }
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
            if ( statement.containsKey("note") ) {
              String note = statement.get("note").replaceAll("\n", "");
              if ( ! note.isEmpty() ) f867.subfields.add(new Subfield(3,'z',note));
            }
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
            if ( statement.containsKey("note") ) {
              String note = statement.get("note").replaceAll("\n", "");
              if ( ! note.isEmpty() ) f868.subfields.add(new Subfield(3,'z',note));
            }
            holdingRec.dataFields.add(f868);
          }
        }

        for ( Item item : values.items.getItems().get(holdingUuid) ) {
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
            List<String> yearCaptions = (ArrayList)item.rawFolioItem.get("yearCaption");
            List<String> nonNulls = new ArrayList<>();
            for (String s : yearCaptions) if (s != null && ! s.isEmpty()) nonNulls.add(s);
            if ( ! nonNulls.isEmpty() )
              f890.subfields.add(new Subfield(8,'y',String.join(" ", nonNulls)));
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
        }
      }
      h.marc = holdingRec;
      bibRec.dataFields.addAll(holdingRec.dataFields);
    }
    return values;
  }

  private void updatePodInventoryForInactiveInstance(
      Map<String,Object> instance, Timestamp bibModdate, Timestamp instanceModdate) throws SQLException {

    String instanceHrid = (String)instance.get("hrid");
    removeOldHoldingAndItemPodExportData(instanceHrid);

    // add various fields flagged as inactive
    this.insertBibPodStmt.setString(1,instanceHrid);
    this.insertBibPodStmt.setTimestamp(2, bibModdate);
    this.insertBibPodStmt.setTimestamp(3, instanceModdate);
    this.insertBibPodStmt.setBoolean(4, false);
    this.insertBibPodStmt.executeUpdate();

    // pipe inactive holding and item data
    this.getHoldingFolioStmt.setString(1, instanceHrid);
    try ( ResultSet rs = this.getHoldingFolioStmt.executeQuery() ) {
      while (rs.next()) {
        String holdingHrid = rs.getString("hrid");
        this.insertHoldingPodStmt.setString(1, holdingHrid);
        this.insertHoldingPodStmt.setString(2, instanceHrid);
        this.insertHoldingPodStmt.setTimestamp(3, rs.getTimestamp("moddate"));
        this.insertHoldingPodStmt.setBoolean(4, false);
        this.insertHoldingPodStmt.setString(5, null);
        this.insertHoldingPodStmt.executeUpdate();

        this.getItemFolioStmt.setString(1, holdingHrid);
        try ( ResultSet rs2 = this.getItemFolioStmt.executeQuery() ) {
          while (rs2.next()) {
            this.insertItemPodStmt.setString(1, rs2.getString("hrid"));
            this.insertItemPodStmt.setString(2, holdingHrid);
            this.insertItemPodStmt.setTimestamp(3, rs2.getTimestamp("moddate"));
            this.insertItemPodStmt.setBoolean(4, false);
            this.insertItemPodStmt.executeUpdate();
          }
        }
      }
    }

  }

  private void updatePodInventoryForActiveInstance(Map<String, Object> instance, Timestamp moddate,
      Timestamp instanceModdate, HoldingsAndItems holdingsAndItems) throws SQLException {
    String instanceHrid = (String)instance.get("hrid");

    this.insertBibPodStmt.setString(1,instanceHrid);
    this.insertBibPodStmt.setTimestamp(2, moddate);
    this.insertBibPodStmt.setTimestamp(3, instanceModdate);
    this.insertBibPodStmt.setBoolean(4, true);
    this.insertBibPodStmt.executeUpdate();

    removeOldHoldingAndItemPodExportData(instanceHrid);

    for ( String holdingUuid : holdingsAndItems.holdings.getUuids() ) {
      Holding h = holdingsAndItems.holdings.get(holdingUuid);
      Map<String,TreeSet<Item>> items = holdingsAndItems.items.getItems();

      this.insertHoldingPodStmt.setString(1, h.hrid);
      this.insertHoldingPodStmt.setString(2, instanceHrid);
      Map<String,String> holdingMetadata = (Map<String,String>)h.rawFolioHolding.get("metadata");
      Timestamp holdingModdate = Timestamp.from(
          isoDT.parse(holdingMetadata.get("updatedDate"),Instant::from));
      this.insertHoldingPodStmt.setTimestamp(3, holdingModdate);
      this.insertHoldingPodStmt.setBoolean(4, h.active);
      this.insertHoldingPodStmt.setString(5,(h.active)?h.marc.toString():null);
      this.insertHoldingPodStmt.executeUpdate();

      if ( items.containsKey(holdingUuid) ) for ( Item i : items.get(holdingUuid) ) {
        this.insertItemPodStmt.setString(1, i.hrid);
        this.insertItemPodStmt.setString(2, h.hrid);
        Map<String,String> itemMetadata = (Map<String,String>)i.rawFolioItem.get("metadata");
        Timestamp itemModdate = ( itemMetadata == null )
            ? Timestamp.valueOf("2020-07-01 00:00:00")
            : Timestamp.from(isoDT.parse(itemMetadata.get("updatedDate"),Instant::from));
        this.insertItemPodStmt.setTimestamp(3, itemModdate);
        this.insertItemPodStmt.setBoolean(4, h.active && i.active);
        this.insertItemPodStmt.addBatch();
      }
      this.insertItemPodStmt.executeBatch();
    }
  }

  private class PreviousBibStatus {
    boolean active;
    Timestamp moddate;
  }
  private PreviousBibStatus getPrevPodStatus(String instanceHrid) throws SQLException {
    this.getPreviousBibStatusStmt.setString(1, instanceHrid);
    try ( ResultSet rs = this.getPreviousBibStatusStmt.executeQuery()) {
      while (rs.next()) {
        PreviousBibStatus p = new PreviousBibStatus();
        p.active = rs.getBoolean("podActive");
        p.moddate = rs.getTimestamp("moddate");
        return p;
      }
    }
    PreviousBibStatus p = new PreviousBibStatus();
    p.active = false;
    return p;
  }

  private void removeOldHoldingAndItemPodExportData(String instanceHrid) throws SQLException {
    Set<String> holdings = new HashSet<>();
    this.getHoldingPodStmt.setString(1, instanceHrid);
    try ( ResultSet rs = this.getHoldingPodStmt.executeQuery() ) { while (rs.next()) {
      String holdingHrid = rs.getString("hrid");
      holdings.add(holdingHrid);
      this.deleteHoldingPodStmt.setString(1, instanceHrid);
      this.deleteHoldingPodStmt.setString(2, holdingHrid);
      this.deleteHoldingPodStmt.executeUpdate();
    } }
    for (String holdingHrid : holdings) {
      this.getItemPodStmt.setString(1, holdingHrid);
      try (ResultSet rs = this.getItemPodStmt.executeQuery()) {
        while ( rs.next() ) {
          this.deleteItemPodStmt.setString(1, holdingHrid);
          this.deleteItemPodStmt.setString(2, rs.getString("hrid"));
          this.deleteItemPodStmt.addBatch();
        }
        this.deleteItemPodStmt.executeBatch();
      }
    }
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

  private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("Z"));
  private static ObjectMapper mapper = new ObjectMapper();
}
