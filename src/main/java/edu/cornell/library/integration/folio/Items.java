package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.LoanTypes.LoanType;
import edu.cornell.library.integration.folio.Locations.Location;

public class Items {

  static Locations locations = null;
  static ReferenceData materialTypes = null;
  static ReferenceData itemNoteTypes = null;

  public static void initialize(OkapiClient okapi, Locations locs) throws IOException {
    if (locations == null) {
      locations = locs;
      materialTypes = new ReferenceData( okapi, "/material-types", "name");
      itemNoteTypes = new ReferenceData( okapi, "/item-note-types", "name");
    }
  }

  public static ItemList retrieveItemsForHoldings(
      OkapiClient okapi, Connection inventory, String bibId, HoldingSet holdings)
          throws SQLException, IOException {
    if (locations == null) {
      locations = new Locations( okapi );
      materialTypes = new ReferenceData( okapi, "/material-types", "name");
      itemNoteTypes = new ReferenceData( okapi, "/item-note-types", "name");
    }
    ItemList il = new ItemList();
    Map<Integer,String> dueDates = new TreeMap<>();
    Map<Integer,String> requests = new TreeMap<>();
    try (PreparedStatement itemByHolding = inventory.prepareStatement(
          "SELECT i.id, i.content, s.sequence"+
          "  FROM itemFolio i LEFT JOIN itemSequence s ON i.hrid = s.hrid" +
          " WHERE i.holdingHrid = ?")) {
    for (String holdingId : holdings.getUuids()) {
      Holding h = holdings.get(holdingId);
      if (h.online != null && h.online) continue;
      itemByHolding.setString(1, h.hrid);
      List<Map<String, Object>> rawItems = new ArrayList<>();
      try (ResultSet rs = itemByHolding.executeQuery()) {
        while (rs.next()) {
          Map<String,Object> rawItem = mapper.readValue(rs.getString("content"), Map.class);
          if ( ! rawItem.containsKey("id") ) rawItem.put("id", rs.getString("id") );
          Integer sequence = rs.getInt("sequence");
          if ( sequence != 0 ) rawItem.put("sequence", sequence);
          rawItems.add(rawItem);
        }
      }

      TreeSet<Item> items = new TreeSet<>();
      for (Map<String, Object> rawItem : rawItems) {
        Item i = new Item(inventory,rawItem,h);
        i.callNumber = h.call;
        items.add(i);
      }
      il.put(holdingId, items);
    }
    if (inventory != null) {
      updateInInventory(inventory,bibId,TrackingTable.DUEDATES,dueDates);
      updateInInventory(inventory,bibId,TrackingTable.REQUESTS,requests);
    }
    return il;
    }
  }
  private static PreparedStatement itemByHolding = null;

  static Item retrieveItemByBarcode ( Connection inventory, String barcode, Holding holding )
      throws SQLException, JsonParseException, JsonMappingException, IOException {
    try ( PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT * FROM itemFolio WHERE barcode = ?")) {
      pstmt.setString(1, barcode);
      try ( ResultSet rs = pstmt.executeQuery() ) {
        while (rs.next()) {
          Item i = new Item(inventory, mapper.readValue(rs.getString("content"),Map.class),holding);
          i.id = rs.getString("id");
          return i;
        }
      }
    }
    return null;
  }

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
      Connection inventory, String bibId,TrackingTable table, Map<Integer, String> details)
          throws SQLException {

    if ( details.isEmpty() ) {
      try (PreparedStatement delStmt = inventory.prepareStatement(
          "DELETE FROM "+table+" WHERE bib_id = ?")) {
        delStmt.setString(1, bibId);
        delStmt.executeUpdate();
      }
      return;      
    }

    String oldJson = null;
    try (PreparedStatement selStmt = inventory.prepareStatement(
        "SELECT json FROM "+table+" WHERE bib_id = ?")) {
      selStmt.setString(1, bibId);
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
      updStmt.setString(1, bibId);
      updStmt.setString(2, json);
      updStmt.executeUpdate();
    }

  }

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

    public Object getStatCodes(ReferenceData statCodesRefData) {
      Set<String> statcodes = new HashSet<>();
      for (TreeSet<Item> items : this.items.values())
        for ( Item i : items) {
          if ( i.rawFolioItem.containsKey("statisticalCodeIds")) {
            List<String> codeUuids = (List<String>)i.rawFolioItem.get("statisticalCodeIds");
            for (String uuid : codeUuids) {
              String code = statCodesRefData.getName(uuid);
              if ( code != null ) statcodes.add("item_"+code);
            }
          }
        }
      return statcodes;
    }
  }

  public static class Item implements Comparable<Item> {

    @JsonProperty("id")        public String id;
    @JsonProperty("hrid")      public final String hrid;
    @JsonProperty("barcode")   public final String barcode;
    @JsonProperty("copy")      private final String copy;
    @JsonProperty("sequence")  private Integer sequence = null;
    @JsonProperty("call")      public String callNumber = null;
    @JsonProperty("enum")      public String enumeration;
    @JsonProperty("chron")     public final String chron;
    @JsonProperty("location")  public Location location = null;
    @JsonProperty("permLocation") public String permLocation = null;
    @JsonProperty("loanType")  public LoanType loanType = null;
    @JsonProperty("matType")   public Map<String,String> matType = null;
    @JsonProperty("rmc")       public Map<String,String> rmc = null;
    @JsonProperty("status")    public ItemStatus status;
    @JsonProperty("empty")     public Boolean empty;
    @JsonProperty("date")      public Integer date;
    @JsonProperty("active")    public boolean active = true;

    @JsonIgnore public Map<String,Object> rawFolioItem = null;

    Item(Connection inventory, Map<String,Object> raw, Holding holding)
        throws SQLException, IOException {

      if (locations == null)
        locations = new Locations();
      this.rawFolioItem = raw;
      this.id = (String)raw.get("id");
      this.hrid = (String)raw.get("hrid");
      String barcode = (String)raw.get("barcode");
      if (barcode == null) { this.empty = true;   this.barcode = null; }
      else {                 this.empty = null;   this.barcode = barcode; }
      this.copy = (String)raw.get("copyNumber");
      if ( raw.containsKey("sequence") )
        this.sequence = (int) raw.get("sequence");
      this.enumeration = (String)raw.get("enumeration");
      this.chron = (String)raw.get("chronology");

      if ( raw.containsKey("temporaryLocationId") )
        this.location = locations.getByUuid( (String)raw.get("temporaryLocationId") );
      else if ( raw.containsKey("permanentLocationId") )
        this.location = locations.getByUuid( (String)raw.get("permanentLocationId") );
      else if (holding != null)
        this.location = holding.location;
      if ( holding != null ) {
        this.permLocation = holding.location.name;
        this.active = holding.active;
      }
      if ( raw.containsKey("discoverySuppress") && (boolean)raw.get("discoverySuppress") )
        this.active = false;

      String loanTypeId = (raw.containsKey("temporaryLoanTypeId"))
          ? (String)raw.get("temporaryLoanTypeId"): (String)raw.get("permanentLoanTypeId");
      this.loanType = LoanTypes.getByUuid(loanTypeId);
      if ( materialTypes != null )
        this.matType = materialTypes.getEntryHashByUuid((String)raw.get("materialTypeId"));
      this.status = new ItemStatus(inventory,raw,this);
      if ( ! raw.containsKey("notes") ) return;
      List<Map<String,String>> notes = (List<Map<String,String>>)raw.get("notes");
      Map<String,String> rmcnotes = new HashMap<>();
      for ( Map<String,String> noteHash : notes ) {
        String type = ( itemNoteTypes != null ) ?
         itemNoteTypes.getName(noteHash.get("itemNoteTypeId")) : null;
        if (type == null)  continue;

        switch (type) {

        case "Vault location":
        case "ArchivesSpace Top Container":
        case "Restrictions":
          rmcnotes.put(type, noteHash.get("note"));
          break;
        }
      }
      if ( rmcnotes.size() > 0 )
        this.rmc = rmcnotes;
    }

    Item(
        @JsonProperty("id")        String id,
        @JsonProperty("hrid")      String hrid,
        @JsonProperty("barcode")   String barcode,
        @JsonProperty("copy")      String copy,
        @JsonProperty("sequence")  Integer sequence,
        @JsonProperty("call")      String callNumber,
        @JsonProperty("enum")      String enumeration,
        @JsonProperty("chron")     String chron,
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
      this.callNumber = callNumber;
      this.enumeration = enumeration;
      this.chron = chron;
      this.location = location;
      this.permLocation = permLocation;
      this.loanType = loanType;
      this.matType = matType;
      this.status = status;
      this.empty = empty;
      this.date = date;
      this.active = active;
    }

    Item(
        String id,
        String hrid,
        String copy,
        String callNumber,
        String enumeration,
        Location location,
        String permLocation,
        LoanType loanType,
        Map<String,String> matType,
        ItemStatus status
        ) {
      this.id = id;
      this.hrid = hrid;
      this.barcode = null;
      this.copy = copy;
      this.sequence = null;
      this.callNumber = callNumber;
      this.enumeration = enumeration;
      this.chron = null;
      this.location = location;
      this.permLocation = permLocation;
      this.loanType = loanType;
      this.matType = matType;
      this.status = status;
      this.active = true;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this);
    }

    @Override
    public int compareTo( final Item other ) {
      if ( this.sequence != null ) {
        if ( other.sequence != null ) {
          int i = this.sequence.compareTo(other.sequence);
          if ( i != 0 ) return i;
          return this.hrid.compareTo(other.hrid);
        }
        return -1;
      }
      if ( other.sequence != null ) return 1;
      return this.hrid.compareTo( other.hrid );
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
      List<String> enumchron = new ArrayList<>();
      if (this.enumeration != null && !this.enumeration.isEmpty()) enumchron.add(this.enumeration);
      if (this.chron != null && !this.chron.isEmpty()) enumchron.add(this.chron);
      if (enumchron.isEmpty())
        return null;
      return String.join(" - ", enumchron);
    }

  }

  public static boolean applyDummyRMCItems(HoldingSet holdings, ItemList items) {
    Map<String,TreeSet<Item>> itemHash = items.getItems() ;
    boolean dummyItemApplied = false;
    for ( String hId : holdings.getUuids() ) {
      if ( itemHash.containsKey(hId) && ! itemHash.get(hId).isEmpty() ) continue;
      Holding h = holdings.get(hId);
      if ( h.active == false ||
           h.location == null ||
           h.location.library == null ||
           ! h.location.library.equals("Kroch Library Rare & Manuscripts"))
        continue;
      // We have an unsuppressed RMC holding with no items
      Item dummy = new Item(
          "holding:"+hId, //id
          "holding:"+h.hrid,//hrid
          (h.copy==null || h.copy.isEmpty())?"1":h.copy,
          h.call,
          (h.holdings==null)?null:String.join("; ",h.holdings),//enum
          h.location,
          h.location.name,//permloc
          LoanTypes.getByName("Non-circulating"),
          materialTypes.getEntryHashByName("unspecified"),
          ItemStatus.AVAIL);
      TreeSet<Item> itemSet = new TreeSet<>();
      itemSet.add(dummy);
      items.put(hId, itemSet);
      dummyItemApplied = true;
    }
    return dummyItemApplied;
  }

}
