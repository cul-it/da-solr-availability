package edu.cornell.library.integration.exports;

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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.DownloadMARC;
import edu.cornell.library.integration.folio.Holding;
import edu.cornell.library.integration.folio.Holdings;
import edu.cornell.library.integration.folio.Items;
import edu.cornell.library.integration.folio.LoanTypes;
import edu.cornell.library.integration.folio.Locations;
import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.folio.ServicePoints;
import edu.cornell.library.integration.folio.GoogleExport.Format;
import edu.cornell.library.integration.folio.GoogleExport.HoldingsAndItems;
import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class GoogleExport {

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("database.properties")){ prop.load(in); }

    try (Connection inventory = DriverManager.getConnection(prop.getProperty("inventoryDBUrl"),
        prop.getProperty("inventoryDBUser"),prop.getProperty("inventoryDBPass")); ){

      OkapiClient okapi = new OkapiClient(prop,"Folio");

      Locations locations = new Locations(okapi);
      ReferenceData holdingsNoteTypes = new ReferenceData(okapi, "/holdings-note-types","name");
      ReferenceData callNumberTypes = new ReferenceData(okapi, "/call-number-types","name");
      LoanTypes.initialize(okapi);
      ServicePoints.initialize(okapi);
      Items.initialize(okapi, locations);

      Set<String> bibs = getBibsToExport(inventory);
      System.out.println("Bib count: "+bibs.size());
      try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("google-sample.xml"))) {
        writer.write("<?xml version='1.0' encoding='UTF-8'?>"
            + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
        int monoCount = 0, serialCount = 0;

        for (String bibid : bibs) {

          Map<String,Object> instance = getInstance( inventory, bibid);
          MarcRecord bibRec = DownloadMARC.getMarc(
              inventory,MarcRecord.RecordType.BIBLIOGRAPHIC,bibid);
          if (bibRec == null) {
            System.out.printf("Skipping non-MARC instance #%s.\n",bibid);
            continue;
          }
          Format format = getFormat(bibRec);
          if ( format.equals(Format.MONO) ) {
            if ( monoCount == 100 ) continue;
          } else if ( format.equals(Format.SERIAL) ) {
            if ( serialCount == 100 ) continue;
          } else {
            System.out.printf("Skipping non mono/serial instance #%s\n",bibid);
            continue;
          }

          // if the bib(instance) is suppressed, we don't want to send to Google.
          if ((instance.containsKey("discoverySuppress") && (boolean)instance.get("discoverySuppress"))
              || (instance.containsKey("staffSuppress") && (boolean)instance.get("staffSuppress")) ) {
            System.out.println(bibid+" inactive due to suppression.");
            continue;
          }

          // if not suppressed, confirm the record isn't NoEx.
          for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
            for (Subfield sf : f.subfields) if (sf.value.contains("NoEx")) {
              System.out.println(bibid+" inactive due NoEx.");
              continue;
            }

          int itemCount = collateHoldingsAndItemsData(
              okapi, inventory, bibRec, locations, holdingsNoteTypes, callNumberTypes);
          if ( itemCount == 0 ) {
            System.out.printf("Skipping Zero Item instance #%s\n",bibid);
            continue;
          }

          if ( format.equals(Format.MONO) ) monoCount++;
          if ( format.equals(Format.SERIAL) ) serialCount++;
          System.out.printf("%s: ( m %d / s %d )\n",bibid,monoCount,serialCount);
          writer.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
              .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
          if ( serialCount == 100 ) break;
        }
        writer.write("</collection>\n");
        writer.flush();
        writer.close();
      }

    }
  }
  
  private static Format getFormat(MarcRecord rec) {
    String recordType =          rec.leader.substring(6,7);
    String bibLevel =  rec.leader.substring(7,8);
    String category = "";
    String typeOfContinuingResource = "";
    for (ControlField f : rec.controlFields)
      switch (f.tag) {
      case "007": if (f.value.length() > 0) category = f.value.substring(0,1); break;
      case "008": if (f.value.length() > 21) typeOfContinuingResource = f.value.substring(21,22);
      }

    if ( category.equals("h") ) return Format.OTHER; // microform
    if ( recordType.equals("a")) {
      if ((bibLevel.equals("a"))
          || (bibLevel.equals("m"))
          || (bibLevel.equals("d"))
          || (bibLevel.equals("c")) )
        return Format.MONO;
      else if ((bibLevel.equals("b"))
          || (bibLevel.equals("s")))
        return Format.SERIAL;
      else if (bibLevel.equals("i")) {
        if (typeOfContinuingResource.equals("w"))
          return Format.OTHER;
        else if (typeOfContinuingResource.equals("m"))
          return Format.MONO;
        else if (typeOfContinuingResource.equals("d"))
          return Format.OTHER;
        else if (typeOfContinuingResource.equals("n") || typeOfContinuingResource.equals("p"))
          return Format.SERIAL;
      }
    } else if (recordType.equals("t") && bibLevel.equals("a") ) {
      return Format.MONO;
    } else if ((recordType.equals("c")) || (recordType.equals("d"))) {
      return Format.SCORE;
    }
    return Format.OTHER;
  }
  private static enum Format { MONO , SERIAL , SCORE,  OTHER}

  private static Set<String> getBibsToExport(Connection inventory) throws SQLException {
    Set<String> bibs = new LinkedHashSet<>();
    try ( Statement stmt = inventory.createStatement() ){
      stmt.setFetchSize(1_000_000);
      try ( ResultSet rs = stmt.executeQuery(
          "SELECT instanceHrid FROM bibFolio ORDER BY RAND() LIMIT 30000")) {
        while ( rs.next() ) bibs.add(rs.getString(1));
      }
    }
    return bibs;
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

  private static class HoldingsAndItems {
    public HoldingSet holdings;
    public ItemList items;
  }


  private static int collateHoldingsAndItemsData(
      OkapiClient okapi, Connection inventory, MarcRecord bibRec, Locations locations,
      ReferenceData holdingsNoteTypes, ReferenceData callNumberTypes)
      throws SQLException, IOException {
    int maxBibFieldId = bibRec.dataFields.last().id;
    HoldingsAndItems values = new HoldingsAndItems();
    values.holdings = Holdings.retrieveHoldingsByInstanceHrid(
        inventory, locations, holdingsNoteTypes, callNumberTypes, bibRec.id);
    values.items = Items.retrieveItemsForHoldings(okapi, inventory, bibRec.id, values.holdings);

    TreeSet<DataField> itemFields = new TreeSet<>();

    for ( String holdingUuid : values.holdings.getUuids()) {
      Holding h = values.holdings.get(holdingUuid);
      if ( h.active == false ) continue;
      if ( h.online != null && h.online == true ) { h.active = false; continue; }

      for ( Item item : values.items.getItems().get(holdingUuid) ) {
        if ( ! item.active ) continue;
        if ( suppressedItemStatuses.contains( item.status.status ) ) continue;
        if ( item.empty != null && item.empty ) continue;
        DataField f955 = new DataField(++maxBibFieldId, "955");
        f955.subfields.add(new Subfield(1,'h',h.hrid));
        f955.subfields.add(new Subfield(2,'i',item.hrid));
        if (h.call != null && ! h.call.isEmpty())
          f955.subfields.add(new Subfield(3,'e',h.call));
        f955.subfields.add(new Subfield(4,'l',item.location.code));
        f955.subfields.add(new Subfield(5,'b',item.barcode));
        if ( item.enumeration != null && ! item.enumeration.isEmpty() )
          f955.subfields.add(new Subfield(6,'v',item.enumeration));
        if ( item.chron != null && ! item.chron.isEmpty() )
          f955.subfields.add(new Subfield(7,'y',item.chron));
        itemFields.add(f955);
      }
      bibRec.dataFields.addAll(itemFields);
      for (DataField f : itemFields)
        System.out.println(f.toString());
    }
    return itemFields.size();
  }

  private static List<String> suppressedItemStatuses = Arrays.asList(
      "Aged to lost","Declared lost","Long missing","Lost and paid","Withdrawn");
  private static List<String> suppressedMaterialTypes = Arrays.asList(
      "BD MATERIAL","Carrel Keys","Computfile");


}
