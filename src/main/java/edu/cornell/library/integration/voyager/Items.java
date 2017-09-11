package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
  static Locations locations = null;
  static ItemTypes itemTypes = null;

  public static void detectChangedItems( Connection voyager ) {

  }

  public static void detectChangedItemStatuses( Connection voyager ) {
    // SELECT * FROM item_STATUS WHERE item_STATUS_DATE > ?
  }

  public static List<Item> retrieveItemsByHoldingId( Connection voyager, int mfhd_id ) throws SQLException {
    if (locations == null) {
      locations = new Locations( voyager );
      itemTypes = new ItemTypes( voyager );
    }
    try (PreparedStatement pstmt = voyager.prepareStatement(itemByMfhdIdQuery)) {
      pstmt.setInt(1, mfhd_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        List<Item> items = new ArrayList<>();
        while (rs.next())
          items.add(new Item(voyager,rs));
        return items;
      }
    }
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
          return new Item(voyager,rs);
      }
    }
    return null;
  }

  public static Item extractItemFromJson( String json ) throws IOException {
    return mapper.readValue(json, Item.class);
  }

  static ObjectMapper mapper = new ObjectMapper();
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  public static class Item {

    @JsonProperty("id")              private final int itemId;
    @JsonProperty("mfhd_id")         private final int mfhdId;
    @JsonProperty("barcode")         private final String barcode;
    @JsonProperty("copy_number")     private final int copyNumber;
    @JsonProperty("sequence_number") private final int sequenceNumber;
    @JsonProperty("year")            private final String year;
    @JsonProperty("chron")           private final String chron;
    @JsonProperty("enum")            private final String enumeration;
    @JsonProperty("caption")         private final String caption;
    @JsonProperty("holds")           private final int holds;
    @JsonProperty("recalls")         private final int recalls;
    @JsonProperty("on_reserve")      private final Boolean onReserve;
    @JsonProperty("location")        private final Location location;
    @JsonProperty("type")            private final ItemType type;
    @JsonProperty("status")          public final ItemStatus status;
    @JsonProperty("date")            public final Integer date;

    private Item(Connection voyager, ResultSet rs) throws SQLException {
      this.itemId = rs.getInt("ITEM_ID");
      this.mfhdId = rs.getInt("MFHD_ID");
      this.barcode = rs.getString("ITEM_BARCODE");

      this.copyNumber = rs.getInt("COPY_NUMBER");
      this.sequenceNumber = rs.getInt("ITEM_SEQUENCE_NUMBER");
      this.year = rs.getString("YEAR");
      this.chron = rs.getString("CHRON");
      this.enumeration = rs.getString("ITEM_ENUM");
      this.caption = rs.getString("CAPTION");
      this.holds = rs.getInt("HOLDS_PLACED");
      this.recalls = rs.getInt("RECALLS_PLACED");
      this.onReserve = (rs.getString("ON_RESERVE").equals("N"))?false:true;
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

    @JsonCreator
    private Item(
        @JsonProperty("id")              int itemId,
        @JsonProperty("mfhd_id")         int mfhdId,
        @JsonProperty("barcode")         String barcode,
        @JsonProperty("copy_number")     int copyNumber,
        @JsonProperty("sequence_number") int sequenceNumber,
        @JsonProperty("year")            String year,
        @JsonProperty("chron")           String chron,
        @JsonProperty("enum")            String enumeration,
        @JsonProperty("caption")         String caption,
        @JsonProperty("holds")           int holds,
        @JsonProperty("recalls")         int recalls,
        @JsonProperty("onReserve")       Boolean onReserve,
        @JsonProperty("location")        Location location,
        @JsonProperty("type")            ItemType type,
        @JsonProperty("status")          ItemStatus status,
        @JsonProperty("date")            Integer date
        ) {
      this.itemId = itemId;
      this.mfhdId = mfhdId;
      this.barcode = barcode;
      this.copyNumber = copyNumber;
      this.sequenceNumber = sequenceNumber;
      this.year = year;
      this.chron = chron;
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
}
