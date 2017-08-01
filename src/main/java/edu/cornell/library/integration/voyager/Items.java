package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.voyager.ItemTypes.ItemType;
import edu.cornell.library.integration.voyager.Locations.Location;

public class Items {

  static final String itemByMfhdIdQuery = 
      "SELECT mfhd_item.*, item.*, item_barcode.item_barcode " +
      " FROM mfhd_item, item" +
      " LEFT OUTER JOIN item_barcode" +
      "      ON item_barcode.item_id = item.item_id "+
      "       AND item_barcode.barcode_status = '1'" +
      " WHERE mfhd_item.mfhd_id = ?" +
      "   AND mfhd_item.item_id = item.item_id" ;
  static final String itemByItemIdQuery = 
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

  static ObjectMapper mapper = new ObjectMapper();
  @SuppressWarnings("unused")
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  public static class Item {

    // BEGIN FIELDS FOR JSON
    private final int item_id;
    private final int mfhd_id;
    private final String item_barcode;
    private final int copy_number;
    private final int item_sequence_number;
    private final String year;
    private final String chron;
    private final String item_enum;
    private final String caption;
    private final int holds_placed;
    private final int recalls_placed;
    private final String on_reserve;
    private final Location location;
    private final ItemType type;
    private final ItemStatus status;
    // END FIELDS FOR JSON

    private Item(Connection voyager, ResultSet rs) throws SQLException {
      this.item_id = rs.getInt("ITEM_ID");
      this.mfhd_id = rs.getInt("MFHD_ID");
      this.item_barcode = rs.getString("ITEM_BARCODE");

      this.copy_number = rs.getInt("COPY_NUMBER");
      this.item_sequence_number = rs.getInt("ITEM_SEQUENCE_NUMBER");
      this.year = rs.getString("YEAR");
      this.chron = rs.getString("CHRON");
      this.item_enum = rs.getString("ITEM_ENUM");
      this.caption = rs.getString("CAPTION");
      this.holds_placed = rs.getInt("HOLDS_PLACED");
      this.recalls_placed = rs.getInt("RECALLS_PLACED");
      this.on_reserve = rs.getString("ON_RESERVE");
      int locationNumber = rs.getInt("TEMP_LOCATION");
      if (0 == locationNumber)
        locationNumber = rs.getInt("PERM_LOCATION");
      this.location = locations.getByNumber(locationNumber);
      int itemTypeId = rs.getInt("TEMP_ITEM_TYPE_ID");
      if (0 == itemTypeId)
        itemTypeId = rs.getInt("ITEM_TYPE_ID");
      this.type = itemTypes.getById(itemTypeId);
      this.status = new ItemStatus( voyager, this.item_id );
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this);
    }
  }
}
