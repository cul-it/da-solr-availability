package edu.cornell.library.integration.exports;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.naming.AuthenticationException;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.DownloadMARC;
import edu.cornell.library.integration.folio.Holding;
import edu.cornell.library.integration.folio.Holdings;
import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Items;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;
import edu.cornell.library.integration.folio.LoanTypes;
import edu.cornell.library.integration.folio.Locations;
import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.folio.ServicePoints;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

public class GoogleExport {

  public static void main(String[] args) throws IOException, SQLException, AuthenticationException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("database.properties")){ prop.load(in); }

    try (Connection inventory = DriverManager.getConnection(prop.getProperty("databaseURLCurrent"),
                   prop.getProperty("databaseUserCurrent"), prop.getProperty("databasePassCurrent")); ){

      OkapiClient okapi = new OkapiClient(prop,"Folio");

      Locations locations = new Locations(okapi);
      ReferenceData holdingsNoteTypes = new ReferenceData(okapi, "/holdings-note-types","name");
      ReferenceData callNumberTypes = new ReferenceData(okapi, "/call-number-types","name");
      LoanTypes.initialize(okapi);
      ServicePoints.initialize(okapi);
      Items.initialize(okapi, locations);

      Set<String> bibs = ExportUtils.getBibsToExport(inventory);
      System.out.println("Bib count: "+bibs.size());
      int fileCount = 0;
      Path outputFile = Paths.get(String.format("cornell-export-%d.xml",++fileCount));
      BufferedWriter writer = Files.newBufferedWriter(outputFile);
      writer.write("<?xml version='1.0' encoding='UTF-8'?>"
          + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

      BIB: for (String bibid : bibs) {

        Map<String,Object> instance = ExportUtils.getInstance( inventory, bibid);

        // if the bib(instance) is suppressed, we don't want to send to Google.
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
        Format format = getFormat(bibRec);
        if ( format.equals(Format.OTHER) ) {
          System.out.printf("Skipping %8s: bib format not on list\n",bibid);
          continue BIB;
        }

        ExportUtils.cleanUnwantedDataFields(bibRec, Arrays.asList("995"), null, false);

        Map<String,Integer> itemStats = collateHoldingsAndItemsData(
            okapi, inventory, bibRec, locations, holdingsNoteTypes, callNumberTypes);
        int items = itemStats.get("items");
        int masterBWs = itemStats.get("master BW items");
        if ( items == 0 ) {
          if ( masterBWs == 0)
            System.out.printf("Skipping %8s: no candidate items\n",bibid);
          else
            System.out.printf(
                "Skipping %8s: no candidate items after elimination of %s master bound-withs\n",
                bibid, masterBWs);
          continue BIB;
        }

        // confirm the record isn't NoEx.
        for (DataField f : bibRec.dataFields) if (f.tag.equals("995"))
          for (Subfield sf : f.subfields) if (sf.value.contains("NoEx")) {
            System.out.printf("Skipping %8s: NoEx\n",bibid);
            continue BIB;
          }

        cleanUp041(bibRec);

        if ( masterBWs == 0 )
          System.out.printf("Exporting %8s\n",bibid);
        else
          System.out.printf(
              "Exporting %8s with %s items after elimination of %s master bound-withs\n",
              bibid, items, masterBWs);

        writer.write(bibRec.toString("xml").replace("<?xml version='1.0' encoding='UTF-8'?>", "")
            .replace(" xmlns=\"http://www.loc.gov/MARC21/slim\"","")+"\n");
        if ( Files.size(outputFile) > 3113851290L /*2.9GB*/ ) {
          writer.write("</collection>\n");
          writer.flush();
          writer.close();
          outputFile = Paths.get(String.format("cornell-export-%d.xml",++fileCount));
          writer = Files.newBufferedWriter(outputFile);
          writer.write("<?xml version='1.0' encoding='UTF-8'?>"
              + "<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");
        }
      }
      writer.write("</collection>\n");
      writer.flush();
      writer.close();

    }
  }

  private static void cleanUp041(MarcRecord rec) {
    Set<DataField> deletes = new HashSet<>();
    Set<DataField> adds = new HashSet<>();
    for (DataField f : rec.dataFields) {
      if ( ! f.tag.equals("041") ) continue;
      boolean fieldNeedsAdjustment = false;
      for (Subfield sf : f.subfields) {
        if (sf.value.length() > 3 && Character.isAlphabetic(sf.code)) {
          fieldNeedsAdjustment = true;
          break;
        }
      }
      if ( ! fieldNeedsAdjustment ) continue;
      DataField newF = new DataField(f.id,f.tag);
      newF.ind1 = f.ind1;
      newF.ind2 = f.ind2;
      int sfId = 0;
      for (Subfield sf : f.subfields) {
        String sfvalue = sf.value.trim();
        if ( ! Character.isAlphabetic(sf.code) || sfvalue.length() == 3) {
          newF.subfields.add(new Subfield(++sfId,sf.code,sfvalue));
          continue;
        }
        if ( sfvalue.length() % 3 == 0 ) {
          boolean mapToH = sf.code.equals('a') && f.ind1.equals('1') && ! fieldContainsCode(f,'h');
          List<String> langCodes = splitString(sfvalue,3);
          for ( int i = 0; i < langCodes.size() ; i++ )
            newF.subfields.add(new Subfield(++sfId,(mapToH && i > 0)?'h':sf.code,langCodes.get(i)));
          continue;
        }
        System.out.println("Non-standard 041 (b"+rec.id+"): "+f.toString());
        newF.subfields.add(new Subfield(++sfId,sf.code,sfvalue));//copying over the bad field
      }
      deletes.add(f);
      adds.add(newF);
      System.out.println(rec.id+": "+f.toString()+" ==> "+newF.toString());
    }
    for (DataField f : deletes) rec.dataFields.remove(f);
    for (DataField newF: adds) rec.dataFields.add(newF);
  }

  private static boolean fieldContainsCode(DataField f, char c) {
    for (Subfield sf : f.subfields) if (sf.code.equals(c)) return true;
    return false;
  }

  private static List<String> splitString ( String s, int lengths ) {
    List<String> chunks = new ArrayList<>((s.length()+lengths-1)/lengths);
    for (int start = 0; start < s.length(); start += lengths) {
      chunks.add(s.substring(start, Math.min(s.length(), start + lengths)));
    }
    return chunks;
  }

  private static Format getFormat(MarcRecord rec) {
    String recordType = rec.leader.substring(6,7);
    String bibLevel = rec.leader.substring(7,8);
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
    } else if (recordType.equals("t")) {
      if ( bibLevel.equals("a") ) return Format.MONO;
      return Format.MANUSCRIPT;
    } else if ((recordType.equals("c")) || (recordType.equals("d"))) {
      return Format.SCORE;
    }
    return Format.OTHER;
  }
  private static enum Format { MONO , SERIAL , SCORE,  OTHER, MANUSCRIPT }

  private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("Z"));

  private static class HoldingsAndItems {
    public HoldingSet holdings;
    public ItemList items;
  }


  private static Map<String,Integer> collateHoldingsAndItemsData(
      OkapiClient okapi, Connection inventory, MarcRecord bibRec, Locations locations,
      ReferenceData holdingsNoteTypes, ReferenceData callNumberTypes)
      throws SQLException, IOException, AuthenticationException {
    int maxBibFieldId = bibRec.dataFields.last().id;
    Map<String,Integer> output = new HashMap<>();
    output.put("master BW items", 0);
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
        if ( suppressedItemStatuses.contains(item.status.status) ) continue;
        if ( suppressedMaterialTypes.contains(item.matType.get("name")) ) continue;
        if ( item.empty != null && item.empty ) continue;
        if ( item.enumeration != null && item.enumeration.toLowerCase().contains("box") ) continue;
        if ( isBoundWith(inventory,bibRec.id,item) ) {
          output.put("master BW items", output.get("master BW items")+1);
          continue;
        }
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
    }
    output.put("items", itemFields.size());
    return output;
  }

  private static boolean isBoundWith(Connection inventory, String bibid, Item item)
      throws SQLException {
    if ( boundWithStmt == null ) boundWithStmt = inventory.prepareStatement(
        "SELECT GROUP_CONCAT(DISTINCT boundWithInstanceHrid) FROM boundWith WHERE masterItemId = ?");
    boundWithStmt.setString(1, item.id);
    try ( ResultSet rs = boundWithStmt.executeQuery() ) {
      while ( rs.next() ) {
        String otherBibs = rs.getString(1);
        if ( otherBibs == null ) return false;
//        System.out.printf("Skipping item %8s (instance %8s): master bound-with; Other bibs (%s)\n",
//            item.hrid, bibid, otherBibs);
        return true;
      }
    }
    return false; // execution cannot get here
  }
  private static PreparedStatement boundWithStmt = null;

  private static List<String> suppressedItemStatuses = Arrays.asList(
      "Aged to lost","Declared lost","Long missing","Lost and paid","Withdrawn");
  private static List<String> suppressedMaterialTypes = Arrays.asList(
      "BD MATERIAL","Carrel Keys","Computfile","Equipment","ILL MATERIAL","Laptop","Locker Keys",
      "Microform","Object","Peripherals","Room Keys","Soundrec","Supplies","Umbrella","Unbound",
      "Visual");


}
