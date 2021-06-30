package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.LoanTypes.LoanType;
import edu.cornell.library.integration.folio.Locations.Location;

public class Items /*TODO implements ChangeDetector */{

  static Locations locations = null;
  static ReferenceData materialTypes = null;
  static ReferenceData itemNoteTypes = null;

/*
  @Override
  public Map<Integer,Set<Change>> detectChanges(
      Connection voyager, Timestamp since ) throws SQLException {

    Map<Integer,Set<Change>> changes = new HashMap<>();

    try ( PreparedStatement pstmt = voyager.prepareStatement(recentItemChangesQuery )){
      pstmt.setTimestamp(1, since);
      pstmt.setTimestamp(2, since);
      try( ResultSet rs = pstmt.executeQuery() ) {
        while (rs.next()) {
          Timestamp modDate = rs.getTimestamp("modify_date");
          Change c;
          if (modDate != null)
            c = new Change(Change.Type.ITEM,rs.getInt("item_id"),"Item modified",modDate,null);
          else
            c = new Change(Change.Type.ITEM,rs.getInt("item_id"),"Item created",rs.getTimestamp("create_date"),null);
          if ( ! changes.containsKey(rs.getInt("bib_id"))) {
            Set<Change> t = new HashSet<>();
            t.add(c);
            changes.put(rs.getInt("bib_id"),t);
          }
          changes.get(rs.getInt("bib_id")).add(c);
        }
      }
    }
    if ( changes.size() > 4 )
      for ( Set<Change> bibChanges : changes.values() ) for ( Change c : bibChanges )
        c.type = Change.Type.ITEM_BATCH;
    return changes;
  }

  public static Map<Integer,String> collectAllCurrentDueDates( Connection voyager )
      throws SQLException, JsonProcessingException {
    Map<Integer,Map<Integer,String>> t = new HashMap<>();
    try ( PreparedStatement pstmt = voyager.prepareStatement(allDueDatesQuery);
        ResultSet rs = pstmt.executeQuery()) {
      while (rs.next()) {
        Integer bibId = rs.getInt(1);
        if (! t.containsKey(bibId) ) t.put(bibId, new TreeMap<>());
        t.get(bibId).put(rs.getInt(2), rs.getTimestamp(3).toLocalDateTime().format(formatter));
      }
    }
    Map<Integer,String> dueDateJsons = new HashMap<>();
    for (Entry<Integer,Map<Integer,String>> e : t.entrySet())
      dueDateJsons.put(e.getKey(), mapper.writeValueAsString(e.getValue()));
    return dueDateJsons;
  }
  */
  private static DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(
      FormatStyle.SHORT,FormatStyle.SHORT);


  public static ItemList retrieveItemsForHoldings(
      OkapiClient okapi, Connection inventory, Integer bibId, HoldingSet holdings) throws SQLException, IOException {
    if (locations == null) {
      locations = new Locations( okapi );
      materialTypes = new ReferenceData( okapi, "/material-types", "name");
      itemNoteTypes = new ReferenceData( okapi, "/item-note-types", "name");
    }
    ItemList il = new ItemList();
    Map<Integer,String> dueDates = new TreeMap<>();
    Map<Integer,String> requests = new TreeMap<>();
    if (itemByHolding == null)
      itemByHolding = inventory.prepareStatement("SELECT * FROM itemFolio WHERE holdingHrid = ?");
    for (String holdingId : holdings.getUuids()) {
      itemByHolding.setString(1, holdings.get(holdingId).hrid);
      List<Map<String, Object>> rawItems = new ArrayList<>();
      try (ResultSet rs = itemByHolding.executeQuery()) {
        while (rs.next()) {
          Map<String,Object> rawItem = mapper.readValue(rs.getString("content"), Map.class);
          if ( ! rawItem.containsKey("id") ) rawItem.put("id", rs.getString("id") );
          rawItems.add(rawItem);
        }
      }

      TreeSet<Item> items = new TreeSet<>();
      for (Map<String, Object> rawItem : rawItems) {
        Item i = new Item(okapi,rawItem,holdings.get(holdingId).active);
        i.callNumber = holdings.get(holdingId).call;
        items.add(i);
/*TODO Tabulation of due dates and requests for change tracking is likely not going to be needed in Folio
        if ( i.status != null ) {
          if ( i.status.due != null )
            dueDates.put(i.itemId, (new Timestamp(i.status.due*1000)).toLocalDateTime().format(formatter));
          for ( StatusCode c : i.status.statuses ) if ( c.name.contains("Request") )
            requests.put(i.itemId, c.name);
        }*/
      }
      il.put(holdingId, items);
    }
    if (inventory != null) {
      updateInInventory(inventory,bibId,TrackingTable.DUEDATES,dueDates);
      updateInInventory(inventory,bibId,TrackingTable.REQUESTS,requests);
    }
    return il;
  }
  private static PreparedStatement itemByHolding = null;

  private static enum TrackingTable {
    DUEDATES("itemDueDates"),
    REQUESTS("itemRequests");
    private String tableName;
    private TrackingTable( String tableName ) {
      this.tableName = tableName;
    }
    @Override
    public String toString() { return this.tableName; }
  }
  private static void updateInInventory(
      Connection inventory, Integer bibId,TrackingTable table, Map<Integer, String> details)
          throws SQLException {

    if ( details.isEmpty() ) {
      try (PreparedStatement delStmt = inventory.prepareStatement(
          "DELETE FROM "+table+" WHERE bib_id = ?")) {
        delStmt.setInt(1, bibId);
        delStmt.executeUpdate();
      }
      return;      
    }

    String oldJson = null;
    try (PreparedStatement selStmt = inventory.prepareStatement(
        "SELECT json FROM "+table+" WHERE bib_id = ?")) {
      selStmt.setInt(1, bibId);
      try (ResultSet rs = selStmt.executeQuery()) {
        while (rs.next()) oldJson = rs.getString(1);
      }
    }
    String json;
    try { json = mapper.writeValueAsString(details);  }
    catch (JsonProcessingException e) { e.printStackTrace(); return; }

    if (json.equals(oldJson)) return;

    try (PreparedStatement updStmt = inventory.prepareStatement(
        "REPLACE INTO "+table+" (bib_id, json) VALUES (?,?)")) {
      updStmt.setInt(1, bibId);
      updStmt.setString(2, json);
      updStmt.executeUpdate();
    }

  }

/*
  static Item retrieveItemByItemId( Connection voyager, int item_id ) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
      circPolicyGroups = new CircPolicyGroups( voyager );
    }
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByItemIdQuery)) {
      pstmt.setInt(1, item_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          return new Item(voyager,rs, true);
      }
    }
    return null;
  }*/

  /*TODO Is this even used by anything? Maybe a test?
  static Item retrieveItemByBarcode( OkapiClient okapi, String barcode) throws SQLException {
    if (locations == null) {
      locations = new Locations( okapi );
    }
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByBarcodeQuery)) {
      pstmt.setString(1, barcode);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          return new Item(voyager,rs, true);
      }
    }
    return null;
  }
  */

  static Item extractItemFromJson( String json ) throws IOException {
    return mapper.readValue(json, Item.class);
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

  public static class ItemList {
    private Map<String,TreeSet<Item>> items;

    @JsonValue
    public Map<String,TreeSet<Item>> getItems() {
      return this.items;
    }

    @JsonCreator
    ItemList( Map<String,TreeSet<Item>> items ) {
      this.items = items;
    }

    public ItemList( ) {
      this.items = new LinkedHashMap<>();
    }

    public static ItemList extractFromJson( String json ) throws IOException {
      return mapper.readValue(json, ItemList.class);
    }

    public void add( Map<String,TreeSet<Item>> itemSet ) {
      this.items.putAll(itemSet);
    }

    void put( String uuid, TreeSet<Item> itemSet ) {
      this.items.put(uuid, itemSet);
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this.items);
    }

    public int mfhdCount() {
      return this.items.size();
    }
    public int itemCount() {
      return this.items.values().stream().mapToInt(p -> p.size()).sum();
    }

    public Item getItem (String holdingUuid, String itemUuid) {
      if (this.items.containsKey(holdingUuid))
        for (Item i : this.items.get(holdingUuid))
          if (i.id.equals(itemUuid))
            return i;
      return null;
    }

    public Set<String> getBarcodes() {
      Set<String> barcodes = new HashSet<>();
      for (TreeSet<Item> items : this.items.values())
        for ( Item i : items)
          if ( i.barcode != null )
            barcodes.add(i.barcode);
      return barcodes;
    }
  }

  public static class Item implements Comparable<Item> {

    @JsonProperty("id")        public final String id;
    @JsonProperty("hrid")      public final String hrid;
    @JsonProperty("barcode")   public final String barcode;
    @JsonProperty("copy")      private final String copy;
    @JsonProperty("sequence")  private int sequence;
    @JsonProperty("onReserve") private Boolean onReserve = null;
    @JsonProperty("call")      public String callNumber = null;
    @JsonProperty("enum")      public String enumeration;
    @JsonProperty("chron")     public final String chron;
    @JsonProperty("year")      public String year;
    @JsonProperty("caption")   private String caption;
    @JsonProperty("holds")     public Integer holds;
    @JsonProperty("recalls")   public Integer recalls;
    @JsonProperty("location")  public final Location location;
    @JsonProperty("permLocation") public final String permLocation;
    @JsonProperty("loanType")  public LoanType loanType;
    @JsonProperty("matType")   public Map<String,String> matType;
    @JsonProperty("rmc")       public Map<String,String> rmc;
    @JsonProperty("status")    public ItemStatus status;
    @JsonProperty("empty")     public Boolean empty;
    @JsonProperty("date")      public Integer date;
    @JsonProperty("active")    public boolean active = true;

    Item(OkapiClient okapi, Map<String,Object> raw, boolean active) throws IOException {

      this.id = (String)raw.get("id");
      this.hrid = (String)raw.get("hrid");
      String barcode = (String)raw.get("barcode");
      if (barcode == null) { this.empty = true;   this.barcode = null; }
      else {                 this.empty = null;   this.barcode = barcode; }
      this.copy = (String)raw.get("copyNumber");
//TODO      this.sequence = rs.getInt("ITEM_SEQUENCE_NUMBER");
      this.enumeration = (String)raw.get("enumeration");
      this.chron = (String)raw.get("chronology");
//TODO      this.year = rs.getString("YEAR");
//TODO      this.caption = rs.getString("CAPTION"); -> mapped to note in Folio
//TODO      this.holds = (rs.getInt("HOLDS_PLACED") == 0)?null:rs.getInt("HOLDS_PLACED");
//TODO      this.recalls = (rs.getInt("RECALLS_PLACED") == 0)?null:rs.getInt("RECALLS_PLACED");
//TODO      this.onReserve = (rs.getString("ON_RESERVE").equals("N"))?null:true;
      String permLocationId = (String)raw.get("permanentLocationId");
      this.permLocation = locations.getByUuid(permLocationId).name;
      String locationId = (raw.containsKey("temporaryLocationId"))? (String)raw.get("temporaryLocationId"): permLocationId;
      this.location = locations.getByUuid(locationId);
//TODO      this.circGrp = circPolicyGroups.getByLocId(locationNumber);
          
      String loanTypeId = (raw.containsKey("temporaryLoanTypeId")) ? (String)raw.get("temporaryLoanTypeId"): (String)raw.get("permanentLoanTypeId");
      this.loanType = LoanTypes.getByUuid(loanTypeId);
      this.matType = materialTypes.getEntryHashByUuid((String)raw.get("materialTypeId"));
      this.status = new ItemStatus(okapi,raw,this);
      //      this.loanType = loanTypes.getByUuid(loanTypeId);
//      this.status = new ItemStatus( voyager, this.itemId, this.type, this.location );
//      this.date = (int)(((rs.getTimestamp("MODIFY_DATE") == null)
//         ? rs.getTimestamp("CREATE_DATE") : rs.getTimestamp("MODIFY_DATE")).getTime()/1000);
      this.active = active;
      if ( ! raw.containsKey("notes") ) return;
      List<Map<String,String>> notes = (List<Map<String,String>>)raw.get("notes");
      for ( Map<String,String> noteHash : notes ) {
        String type = itemNoteTypes.getUuid(noteHash.get("noteTypeId"));
        if (type == null)  continue;
        Map<String,String> rmcnotes = new HashMap<>();

        switch (type) {

        case "Vault location":
        case "ArchiveSpace Top Container":
        case "Restrictions":
          rmcnotes.put(type, noteHash.get("note"));
          break;
        }
        if ( rmcnotes.size() > 0 )
          this.rmc = rmcnotes;

      }
    }

    Item(
        @JsonProperty("id")        String id,
        @JsonProperty("hrid")      String hrid,
        @JsonProperty("barcode")   String barcode,
        @JsonProperty("copy")      String copy,
        @JsonProperty("sequence")  int sequence,
        @JsonProperty("onReserve") Boolean onReserve,
        @JsonProperty("call")      String callNumber,
        @JsonProperty("enum")      String enumeration,
        @JsonProperty("chron")     String chron,
        @JsonProperty("year")      String year,
        @JsonProperty("caption")   String caption,
        @JsonProperty("holds")     Integer holds,
        @JsonProperty("recalls")   Integer recalls,
        @JsonProperty("location")  Location location,
        @JsonProperty("permLocation") String permLocation,
        @JsonProperty("loanType")  LoanType loanType,
        @JsonProperty("matType")   Map<String,String> matType,
        @JsonProperty("status")    ItemStatus status,
        @JsonProperty("empty")     Boolean empty,
        @JsonProperty("date")      Integer date,
        @JsonProperty("active")    boolean active
        ) {
      this.id = id;
      this.hrid = hrid;
      this.barcode = barcode;
      this.copy = copy;
      this.sequence = sequence;
      this.onReserve = onReserve;
      this.callNumber = callNumber;
      this.enumeration = enumeration;
      this.chron = chron;
      this.year = year;
      this.caption = caption;
      this.holds = holds;
      this.recalls = recalls;
      this.location = location;
      this.permLocation = permLocation;
      this.loanType = loanType;
      this.matType = matType;
      this.status = status;
      this.empty = empty;
      this.date = date;
      this.active = active;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this);
    }

    @Override
    public int compareTo( final Item other ) {
      return this.id.compareTo( other.id );
    }

    @Override
    public int hashCode() {
      return this.id.hashCode();
    }

    @Override
    public boolean equals ( final Object o ) {
      if (this == o) return true;
      if (o == null) return false;
      if ( ! this.getClass().equals(o.getClass()) ) return false;
      return this.id.equals(((Item)o).id);
    }

    String concatEnum() {
      List<String> enumchronyear = new ArrayList<>();
      if (this.enumeration != null && !this.enumeration.isEmpty()) enumchronyear.add(this.enumeration);
      if (this.chron != null && !this.chron.isEmpty()) enumchronyear.add(this.chron);
      if (this.year != null && !this.year.isEmpty()) enumchronyear.add(this.year);
      if (enumchronyear.isEmpty())
        return null;
      return String.join(" - ", enumchronyear);
    }

  }

}
