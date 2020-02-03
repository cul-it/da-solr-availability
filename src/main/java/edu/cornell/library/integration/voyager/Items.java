package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.changes.ChangeDetector;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.ItemStatuses.StatusCode;
import edu.cornell.library.integration.voyager.ItemTypes.ItemType;
import edu.cornell.library.integration.voyager.Locations.Location;

public class Items implements ChangeDetector {

  private static final String itemByMfhdIdQuery = 
      "SELECT mfhd_item.*, item.*, item_barcode.item_barcode " +
      " FROM mfhd_item, item" +
      " LEFT OUTER JOIN item_barcode" +
      "      ON item_barcode.item_id = item.item_id "+
      "       AND item_barcode.barcode_status = '1'" +
      " WHERE mfhd_item.mfhd_id = ?" +
      "   AND mfhd_item.item_id = item.item_id"+
      " ORDER BY mfhd_item.item_id";
  private static final String itemByItemIdQuery = 
      "SELECT mfhd_item.*, item.*, item_barcode.item_barcode " +
      " FROM mfhd_item, item" +
      " LEFT OUTER JOIN item_barcode" +
      "      ON item_barcode.item_id = item.item_id "+
      "       AND item_barcode.barcode_status = '1'" +
      " WHERE item.item_id = ?" +
      "   AND mfhd_item.item_id = item.item_id" ;
  private static final String itemByBarcodeQuery = 
      "SELECT mfhd_item.*, item.*, item_barcode.item_barcode " +
      " FROM mfhd_item, item, item_barcode" +
      " WHERE mfhd_item.item_id = item.item_id" +
      "   AND item_barcode.item_id = item.item_id "+
      "   AND item_barcode.item_barcode = ?";
  private static final String allDueDatesQuery =
      "select bm.bib_id, ct.item_id, ct.current_due_date"+
      "  from circ_transactions ct, mfhd_item mi, mfhd_master mm, bib_mfhd bm, bib_master bmast"+
      " where bm.mfhd_id = mi.mfhd_id"+
      "   and mi.item_id = ct.item_id"+
      "   and bm.bib_id = bmast.bib_id"+
      "   and mm.mfhd_id = mi.mfhd_id"+
      "   and bmast.suppress_in_opac = 'N'"+
      "   and mm.suppress_in_opac = 'N'";
  private static final String recentItemChangesQuery =
      "select bib_master.bib_id, item.modify_date, item.create_date, item.item_id"+
      "  from item, mfhd_item, bib_mfhd, bib_master, mfhd_master "+
      " where bib_master.bib_id = bib_mfhd.bib_id"+
      "   and bib_mfhd.mfhd_id = mfhd_item.mfhd_id"+
      "   and mfhd_item.item_id = item.item_id"+
      "   and mfhd_item.mfhd_id = mfhd_master.mfhd_id"+
      "   and (item.modify_date > ?"+
      "       or item.create_date > ?)"+
      "   and bib_master.suppress_in_opac = 'N'"+
      "   and mfhd_master.suppress_in_opac = 'N'";
  static Locations locations = null;
  static ItemTypes itemTypes = null;
  static CircPolicyGroups circPolicyGroups = null;

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
  private static DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(
      FormatStyle.SHORT,FormatStyle.SHORT);

  public static ItemList retrieveItemsByHoldingId( Connection voyager, int mfhd_id ) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
      circPolicyGroups = new CircPolicyGroups( voyager );
    }
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByMfhdIdQuery)) {
      pstmt.setInt(1, mfhd_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        TreeSet<Item> items = new TreeSet<>();
        while (rs.next())
          items.add(new Item(voyager,rs, false));
        Map<Integer,TreeSet<Item>> itemList = new LinkedHashMap<>();
        itemList.put(mfhd_id, items);
        return new ItemList(itemList);
      }
    }
  }

  public static ItemList retrieveItemsForHoldings(
      Connection voyager, Connection inventory, Integer bibId, HoldingSet holdings) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
      circPolicyGroups = new CircPolicyGroups( voyager );
    }
    ItemList il = new ItemList();
    Map<Integer,String> dueDates = new TreeMap<>();
    Map<Integer,String> requests = new TreeMap<>();
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByMfhdIdQuery)) {
      pstmt.setFetchSize(10_000);
      for (int mfhd_id : holdings.getMfhdIds()) {
        pstmt.setInt(1, mfhd_id);
        try (ResultSet rs = pstmt.executeQuery()) {
          TreeSet<Item> items = new TreeSet<>();
          while (rs.next()) {
            Item i = new Item(voyager,rs,false);
            items.add(i);
            if ( i.status != null ) {
              if ( i.status.due != null )
                dueDates.put(i.itemId, (new Timestamp(i.status.due*1000)).toLocalDateTime().format(formatter));
              for ( StatusCode c : i.status.statuses ) if ( c.name.contains("Request") )
                requests.put(i.itemId, c.name);
            }
          }
          il.put(mfhd_id, items);
        }
      }
    }
    if (inventory != null) {
      updateInInventory(inventory,bibId,TrackingTable.DUEDATES,dueDates);
      updateInInventory(inventory,bibId,TrackingTable.REQUESTS,requests);
    }
    return il;
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
  }

  static Item retrieveItemByBarcode( Connection voyager, String barcode) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
      circPolicyGroups = new CircPolicyGroups( voyager );
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

  static Item extractItemFromJson( String json ) throws IOException {
    return mapper.readValue(json, Item.class);
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

  public static class ItemList {
    private Map<Integer,TreeSet<Item>> items;

    @JsonValue
    public Map<Integer,TreeSet<Item>> getItems() {
      return this.items;
    }

    @JsonCreator
    ItemList( Map<Integer,TreeSet<Item>> items ) {
      this.items = items;
    }

    public ItemList( ) {
      this.items = new LinkedHashMap<>();
    }

    public static ItemList extractFromJson( String json ) throws IOException {
      return mapper.readValue(json, ItemList.class);
    }

    public void add( Map<Integer,TreeSet<Item>> itemSet ) {
      this.items.putAll(itemSet);
    }

    void put( Integer mfhd_id, TreeSet<Item> itemSet ) {
      this.items.put(mfhd_id, itemSet);
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

    public Item getItem (int mfhd_id, int item_id) {
      if (this.items.containsKey(mfhd_id))
        for (Item i : this.items.get(mfhd_id))
          if (i.itemId == item_id)
            return i;
      return null;
    }
  }

  public static class Item implements Comparable<Item> {

    @JsonProperty("id")        public final int itemId;
    @JsonProperty("mfhdId")    public Integer mfhdId;
    @JsonProperty("copy")      private final int copy;
    @JsonProperty("sequence")  private final int sequence;
    @JsonProperty("enum")      public String enumeration;
    @JsonProperty("chron")     public final String chron;
    @JsonProperty("year")      public final String year;
    @JsonProperty("caption")   private final String caption;
    @JsonProperty("holds")     public final Integer holds;
    @JsonProperty("recalls")   public final Integer recalls;
    @JsonProperty("onReserve") private final Boolean onReserve;
    @JsonProperty("location")  public final Location location;
    @JsonProperty("circGrp")   public final Map<Integer,String> circGrp;
    @JsonProperty("type")      public final ItemType type;
    @JsonProperty("status")    public ItemStatus status;
    @JsonProperty("empty")     public Boolean empty;
    @JsonProperty("date")      public final Integer date;

    Item(Connection voyager, ResultSet rs, boolean includeMfhdId) throws SQLException {
      this.itemId = rs.getInt("ITEM_ID");
      this.mfhdId = (includeMfhdId)?rs.getInt("MFHD_ID"):null;

      this.copy = rs.getInt("COPY_NUMBER");
      this.sequence = rs.getInt("ITEM_SEQUENCE_NUMBER");
//      this.enumeration = concatEnum(rs.getString("ITEM_ENUM"),rs.getString("CHRON"),rs.getString("YEAR"));
      this.enumeration = rs.getString("ITEM_ENUM");
      this.chron = rs.getString("CHRON");
      this.year = rs.getString("YEAR");
      this.caption = rs.getString("CAPTION");
      this.holds = (rs.getInt("HOLDS_PLACED") == 0)?null:rs.getInt("HOLDS_PLACED");
      this.recalls = (rs.getInt("RECALLS_PLACED") == 0)?null:rs.getInt("RECALLS_PLACED");
      this.onReserve = (rs.getString("ON_RESERVE").equals("N"))?null:true;
      int locationNumber = rs.getInt("TEMP_LOCATION");
      if (0 == locationNumber)
        locationNumber = rs.getInt("PERM_LOCATION");
      this.location = locations.getByNumber(locationNumber);
      this.circGrp = circPolicyGroups.getByLocId(locationNumber);
      int itemTypeId = rs.getInt("TEMP_ITEM_TYPE_ID");
      if (0 == itemTypeId)
        itemTypeId = rs.getInt("ITEM_TYPE_ID");
      this.type = itemTypes.getById(itemTypeId);
      this.status = new ItemStatus( voyager, this.itemId, this.type, this.location );
      this.date = (int)(((rs.getTimestamp("MODIFY_DATE") == null)
          ? rs.getTimestamp("CREATE_DATE") : rs.getTimestamp("MODIFY_DATE")).getTime()/1000);
      this.empty = (rs.getString("ITEM_BARCODE") == null)?true:null;
    }

    Item(
        @JsonProperty("id")        int itemId,
        @JsonProperty("mfhdId")    Integer mfhdId,
        @JsonProperty("copy")      int copy,
        @JsonProperty("sequence")  int sequence,
        @JsonProperty("enum")      String enumeration,
        @JsonProperty("chron")     String chron,
        @JsonProperty("year")      String year,
        @JsonProperty("caption")   String caption,
        @JsonProperty("holds")     Integer holds,
        @JsonProperty("recalls")   Integer recalls,
        @JsonProperty("onReserve") Boolean onReserve,
        @JsonProperty("location")  Location location,
        @JsonProperty("circGrp")   Map<Integer,String> circGrp,
        @JsonProperty("type")      ItemType type,
        @JsonProperty("status")    ItemStatus status,
        @JsonProperty("date")      Integer date
        ) {
      this.itemId = itemId;
      this.mfhdId = mfhdId;
      this.copy = copy;
      this.sequence = sequence;
      this.enumeration = enumeration;
      this.chron = chron;
      this.year = year;
      this.caption = caption;
      this.holds = holds;
      this.recalls = recalls;
      this.onReserve = onReserve;
      this.location = location;
      this.circGrp = circGrp;
      this.type = type;
      this.status = status;
      this.date = date;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this);
    }

    @Override
    public int compareTo( final Item other ) {
      if (this.sequence == other.sequence) {
        return Integer.compare(this.itemId, other.itemId);
      }
      return Integer.compare(this.sequence, other.sequence);
    }

    @Override
    public int hashCode() {
      return Integer.hashCode( this.itemId );
    }

    @Override
    public boolean equals ( final Object o ) {
      if (this == o) return true;
      if (o == null) return false;
      if ( ! this.getClass().equals(o.getClass()) ) return false;
      return this.itemId == ((Item)o).itemId;
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
