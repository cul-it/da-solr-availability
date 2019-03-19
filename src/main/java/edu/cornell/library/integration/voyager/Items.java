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

import edu.cornell.library.integration.availability.RecordsToSolr.Change;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.ItemTypes.ItemType;
import edu.cornell.library.integration.voyager.Locations.Location;

public class Items {

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
  private static final String recentItemStatusChangesQuery =
      "SELECT * FROM item_status WHERE item_status_date > ?";
  private static final String recentItemReserveStatusChangesQuery =
      "SELECT item_id, effect_date, expire_date"+
      "  FROM reserve_item_history"+
      " WHERE effect_date > ? OR expire_date > ?";
  private static final String bibIdFromItemIdQuery =
      "SELECT b.bib_id       "+
      "  FROM bib_master b,  "+
      "       bib_mfhd bm,   "+
      "       mfhd_master m, "+
      "       mfhd_item mi   "+
      " WHERE b.suppress_in_opac = 'N'  "+
      "   AND b.bib_id = bm.bib_id      "+
      "   AND bm.mfhd_id = m.mfhd_id    "+
      "   AND m.suppress_in_opac = 'N'  "+
      "   AND m.mfhd_id = mi.mfhd_id    "+
      "   AND mi.item_id = ?            ";
  private static final String allDueDatesQuery =
      "select bm.bib_id, ct.item_id, ct.current_due_date"+
      "  from circ_transactions ct, mfhd_item mi, bib_mfhd bm, bib_master bmast"+
      " where bm.mfhd_id = mi.mfhd_id"+
      "   and mi.item_id = ct.item_id"+
      "   and bm.bib_id = bmast.bib_id"+
      "   and bmast.suppress_in_opac = 'N'";
  private static final String recentItemChangesQuery =
      "select bib_id, item.modify_date, item.create_date, item.item_id"+
      "  from item, mfhd_item, bib_mfhd, bib_master "+
      " where bib_master.bib_id = bib_mfhd.bib_id"+
      "   and bib_mfhd.mfhd_id = mfhd_item.mfhd_id"+
      "   and mfhd_item.item_id = item.item_id"+
      "   and (item.modify_date > ?"+
      "       or item.create_date > ?)"+
      "   and bib_master.suppress_in_opac = 'N'";
  private static Locations locations = null;
  private static ItemTypes itemTypes = null;
  private static CircPolicyGroups circPolicyGroups = null;

  public static Map<Integer,Set<Change>> detectChangedItems(
      Connection voyager, Timestamp since, Map<Integer,Set<Change>> changedBibs ) throws SQLException {

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
          if ( ! changedBibs.containsKey(rs.getInt("bib_id"))) {
            Set<Change> t = new HashSet<>();
            t.add(c);
            changedBibs.put(rs.getInt("bib_id"),t);
          }
          changedBibs.get(rs.getInt("bib_id")).add(c);
        }
      }
    }
    return changedBibs;

  }

  public static Map<Integer,Set<Change>> detectChangedItemStatuses(
      Connection voyager, Timestamp since, Map<Integer,Set<Change>> changes ) throws SQLException {

    try (PreparedStatement pstmt = voyager.prepareStatement(recentItemStatusChangesQuery);
        PreparedStatement bibStmt = voyager.prepareStatement(bibIdFromItemIdQuery)) {

      pstmt.setTimestamp(1, since);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          Item item = retrieveItemByItemId( voyager, rs.getInt("item_id"));
          if ( item == null ) {
            System.out.printf("It looks like an item (%d) was deleted right after changing status.\n",rs.getInt("item_id"));
            continue;
          }
          bibStmt.setInt(1, item.itemId);
          try( ResultSet bibRs = bibStmt.executeQuery() ) {
            while (bibRs.next()) {
              Change c = new Change(Change.Type.CIRC,item.itemId,
                  String.format( "%s %s",((item.enumeration!= null)?item.itemId+" ("+item.enumeration+")":item.itemId),
                      item.status.code.values().iterator().next()),
                  new Timestamp(item.status.date*1000),
                  item.location.name);
              Integer bibId = bibRs.getInt(1);
              if (changes.containsKey(bibId))
                changes.get(bibId).add(c);
              else {
                Set<Change> t = new HashSet<>();
                t.add(c);
                changes.put(bibId,t);
              }
            }
          }
        }
      }
    }
    return changes;
  }

  public static Map<Integer,Set<Change>> detectChangedItemDueDates(
      Connection voyager, Map<Integer,String> prevValues, Map<Integer,Set<Change>> changes )
          throws SQLException, JsonProcessingException {
    Map<Integer,String> allDueDates = collectAllCurrentDueDates( voyager );
    for (Integer bibId : prevValues.keySet()) {
      if (allDueDates.containsKey(bibId)) {
        if (! prevValues.get(bibId).equals(allDueDates.get(bibId))) {
          Set<Change> t = new HashSet<>();
          t.add(new Change(Change.Type.CIRC,null,
              "Due Date Modified "+prevValues.get(bibId)+" ==> "+allDueDates.get(bibId), null, null));
          changes.put(bibId, t);
        }
        allDueDates.remove(bibId);
      } else {
        Set<Change> t = new HashSet<>();
        t.add(new Change(Change.Type.CIRC,null,"Due Date Disappeared "+prevValues.get(bibId), null, null));
        changes.put(bibId, t);
      }
    }
    for (Integer bibId : allDueDates.keySet()) {
      Set<Change> t = new HashSet<>();
      t.add(new Change(Change.Type.CIRC,null,"Due Date Appeared "+allDueDates.get(bibId),null,null));
      changes.put(bibId,t);
    }
    return changes;
  }

  public static Map<Integer,Set<Change>> detectChangedItemReserveStatuses(
      Connection voyager, Timestamp since, Map<Integer,Set<Change>> changes ) throws SQLException {

    try (PreparedStatement pstmt = voyager.prepareStatement(recentItemReserveStatusChangesQuery);
        PreparedStatement bibStmt = voyager.prepareStatement(bibIdFromItemIdQuery)) {

      pstmt.setTimestamp(1,since);
      pstmt.setTimestamp(2,since);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          Item item = retrieveItemByItemId( voyager, rs.getInt("item_id"));
          Timestamp modDate = rs.getTimestamp(3);
          if (modDate == null) modDate = rs.getTimestamp(2);
          bibStmt.setInt(1, item.itemId);
          try( ResultSet bibRs = bibStmt.executeQuery() ) {
            while (bibRs.next()) {
              Change c = new Change(Change.Type.RESERVE,item.itemId,
                  ((item.enumeration!= null)?item.itemId+" ("+item.enumeration+")":String.valueOf(item.itemId)),
                  modDate, item.location.name);
              Integer bibId = bibRs.getInt(1);
              if (changes.containsKey(bibId))
                changes.get(bibId).add(c);
              else {
                Set<Change> t = new HashSet<>();
                t.add(c);
                changes.put(bibId,t);
              }
            }
          }
        }
      }
    }
    return changes;
  }

  private static Map<Integer,String> collectAllCurrentDueDates( Connection voyager )
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
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByMfhdIdQuery)) {
      for (int mfhd_id : holdings.getMfhdIds()) {
        pstmt.setInt(1, mfhd_id);
        try (ResultSet rs = pstmt.executeQuery()) {
          TreeSet<Item> items = new TreeSet<>();
          while (rs.next()) {
            Item i = new Item(voyager,rs,false);
            items.add(i);
            if ( i.status != null && i.status.due != null )
              dueDates.put(i.itemId, (new Timestamp(i.status.due*1000)).toLocalDateTime().format(formatter));
          }
          il.put(mfhd_id, items);
        }
      }
    }
    if (inventory != null)
      updateDueDatesInInventory(inventory,bibId,dueDates);
    return il;
  }

  private static void updateDueDatesInInventory(
      Connection inventory, Integer bibId, Map<Integer, String> dueDates) throws SQLException {

    if ( dueDates.isEmpty() ) {
      try (PreparedStatement delStmt = inventory.prepareStatement(
          "DELETE FROM itemDueDates WHERE bib_id = ?")) {
        delStmt.setInt(1, bibId);
        delStmt.executeUpdate();
      }
      return;      
    }

    String oldJson = null;
    try (PreparedStatement selStmt = inventory.prepareStatement(
        "SELECT json FROM itemDueDates WHERE bib_id = ?")) {
      selStmt.setInt(1, bibId);
      try (ResultSet rs = selStmt.executeQuery()) {
        while (rs.next()) oldJson = rs.getString(1);
      }
    }
    String json;
    try { json = mapper.writeValueAsString(dueDates);  }
    catch (JsonProcessingException e) { e.printStackTrace(); return; }

    if (json.equals(oldJson)) return;

    try (PreparedStatement updStmt = inventory.prepareStatement(
        "REPLACE INTO itemDueDates (bib_id, json) VALUES (?,?)")) {
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

  private static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

  public static class ItemList {
    private Map<Integer,TreeSet<Item>> items;

    @JsonValue
    public Map<Integer,TreeSet<Item>> getItems() {
      return items;
    }

    @JsonCreator
    private ItemList( Map<Integer,TreeSet<Item>> items ) {
      this.items = items;
    }

    public ItemList( ) {
      this.items = new LinkedHashMap<>();
    }

    public static ItemList extractFromJson( String json ) throws IOException {
      return mapper.readValue(json, ItemList.class);
    }

    public void add( Map<Integer,TreeSet<Item>> items ) {
      this.items.putAll(items);
    }

    private void put( Integer mfhd_id, TreeSet<Item> items ) {
      this.items.put(mfhd_id, items);
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this.items);
    }

    public int mfhdCount() {
      return items.size();
    }
    public int itemCount() {
      return items.values().stream().mapToInt(p -> p.size()).sum();
    }

    public Item getItem (int mfhd_id, int item_id) {
      if (items.containsKey(mfhd_id))
        for (Item i : items.get(mfhd_id))
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

    private Item(Connection voyager, ResultSet rs, boolean includeMfhdId) throws SQLException {
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
      this.status = new ItemStatus( voyager, this.itemId, this.type );
      this.date = (int)(((rs.getTimestamp("MODIFY_DATE") == null)
          ? rs.getTimestamp("CREATE_DATE") : rs.getTimestamp("MODIFY_DATE")).getTime()/1000);
      this.empty = (rs.getString("ITEM_BARCODE") == null)?true:null;
    }

    private Item(
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
