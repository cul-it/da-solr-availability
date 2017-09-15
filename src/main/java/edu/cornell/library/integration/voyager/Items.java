package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
  static Locations locations = null;
  static ItemTypes itemTypes = null;

  public static void detectChangedItems( Connection voyager ) {

  }

  public static void detectChangedItemStatuses( Connection voyager ) {
    // SELECT * FROM item_STATUS WHERE item_STATUS_DATE > ?
  }

  public static ItemList retrieveItemsByHoldingId( Connection voyager, int mfhd_id ) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
    }
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByMfhdIdQuery)) {
      pstmt.setInt(1, mfhd_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        List<Item> items = new ArrayList<>();
        while (rs.next())
          items.add(new Item(voyager,rs, false));
        Map<Integer,List<Item>> itemList = new HashMap<>();
        itemList.put(mfhd_id, items);
        return new ItemList(itemList);
      }
    }
  }

  public static ItemList retrieveItemsByHoldingIds( Connection voyager, List<Integer> mfhd_ids ) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
    }
    ItemList il = new ItemList();
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByMfhdIdQuery)) {
      for (int mfhd_id : mfhd_ids) {
        pstmt.setInt(1, mfhd_id);
        try (ResultSet rs = pstmt.executeQuery()) {
          List<Item> items = new ArrayList<>();
          while (rs.next())
            items.add(new Item(voyager,rs, false));
          il.put(mfhd_id, items);
        }
      }
    }
    return il;
  }


  public static Item retrieveItemByItemId( Connection voyager, int item_id ) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
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

  public static Item retrieveItemByBarcode( Connection voyager, String barcode) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
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

  public static Item extractItemFromJson( String json ) throws IOException {
    return mapper.readValue(json, Item.class);
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }

  public static class ItemList {
    private Map<Integer,List<Item>> items;

    @JsonValue
    public Map<Integer,List<Item>> getItems() {
      return items;
    }

    @JsonCreator
    public ItemList( Map<Integer,List<Item>> items ) {
      this.items = items;
    }

    public ItemList( ) {
      this.items = new LinkedHashMap<>();
    }

    public void add( Map<Integer,List<Item>> items ) {
      this.items.putAll(items);
    }

    public void put( Integer mfhd_id, List<Item> items ) {
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

  public static class Item {

    @JsonProperty("id")          public final int itemId;
    @JsonProperty("mfhdId")      public Integer mfhdId;
    @JsonProperty("copyNum")     private final int copyNumber;
    @JsonProperty("sequenceNum") private final int sequenceNumber;
    @JsonProperty("enum")        public final String enumeration;
    @JsonProperty("caption")     private final String caption;
    @JsonProperty("holds")       private final Integer holds;
    @JsonProperty("recalls")     private final Integer recalls;
    @JsonProperty("onReserve")   private final Boolean onReserve;
    @JsonProperty("location")    private final Location location;
    @JsonProperty("type")        private final ItemType type;
    @JsonProperty("status")      public final ItemStatus status;
    @JsonProperty("date")        public final Integer date;

    private Item(Connection voyager, ResultSet rs, boolean includeMfhdId) throws SQLException {
      this.itemId = rs.getInt("ITEM_ID");
      this.mfhdId = (includeMfhdId)?rs.getInt("MFHD_ID"):null;

      this.copyNumber = rs.getInt("COPY_NUMBER");
      this.sequenceNumber = rs.getInt("ITEM_SEQUENCE_NUMBER");
      this.enumeration = concatEnum(rs.getString("ITEM_ENUM"),rs.getString("CHRON"),rs.getString("YEAR"));;
      this.caption = rs.getString("CAPTION");
      this.holds = (rs.getInt("HOLDS_PLACED") == 0)?null:rs.getInt("HOLDS_PLACED");
      this.recalls = (rs.getInt("RECALLS_PLACED") == 0)?null:rs.getInt("RECALLS_PLACED");
      this.onReserve = (rs.getString("ON_RESERVE").equals("N"))?null:true;
      int locationNumber = rs.getInt("TEMP_LOCATION");
      if (0 == locationNumber)
        locationNumber = rs.getInt("PERM_LOCATION");
      this.location = locations.getByNumber(locationNumber);
      int itemTypeId = rs.getInt("TEMP_ITEM_TYPE_ID");
      if (0 == itemTypeId)
        itemTypeId = rs.getInt("ITEM_TYPE_ID");
      this.type = itemTypes.getById(itemTypeId);
      this.status = new ItemStatus( voyager, this.itemId );
      this.date = (int)(((rs.getTimestamp("MODIFY_DATE") == null)
          ? rs.getTimestamp("CREATE_DATE") : rs.getTimestamp("MODIFY_DATE")).getTime()/1000);
    }

    private Item(
        @JsonProperty("id")          int itemId,
        @JsonProperty("mfhdId")      Integer mfhdId,
        @JsonProperty("copyNum")     int copyNumber,
        @JsonProperty("sequenceNum") int sequenceNumber,
        @JsonProperty("enum")        String enumeration,
        @JsonProperty("caption")     String caption,
        @JsonProperty("holds")       Integer holds,
        @JsonProperty("recalls")     Integer recalls,
        @JsonProperty("onReserve")   Boolean onReserve,
        @JsonProperty("location")    Location location,
        @JsonProperty("type")        ItemType type,
        @JsonProperty("status")      ItemStatus status,
        @JsonProperty("date")        Integer date
        ) {
      this.itemId = itemId;
      this.mfhdId = mfhdId;
      this.copyNumber = copyNumber;
      this.sequenceNumber = sequenceNumber;
      this.enumeration = enumeration;
      this.caption = caption;
      this.holds = holds;
      this.recalls = recalls;
      this.onReserve = onReserve;
      this.location = location;
      this.type = type;
      this.status = status;
      this.date = date;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this);
    }
  }
  public static String concatEnum(String enumeration, String chron, String year) {
    List<String> enumchronyear = new ArrayList<>();
    if (enumeration != null && !enumeration.isEmpty()) enumchronyear.add(enumeration);
    if (chron != null && !chron.isEmpty()) enumchronyear.add(chron);
    if (year != null && !year.isEmpty()) enumchronyear.add(year);
    if (enumchronyear.isEmpty())
      return null;
    return String.join(" - ", enumchronyear);
  }
}
