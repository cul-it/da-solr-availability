package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.Items.Item;

public class BoundWith {

  @JsonProperty("masterItemId") public final int masterItemId;
  @JsonProperty("masterBibId")  public final int masterBibId;
  @JsonProperty("masterTitle")  public final String masterTitle;
  @JsonProperty("masterEnum")   public final String masterEnum;
  @JsonProperty("thisEnum")     public final String thisEnum;
  @JsonProperty("barcode")      public final String barcode;
  @JsonProperty("status")       public final ItemStatus status;

  public static BoundWith from876Field( Connection voyager, Connection solrInventory, DataField f )
      throws SQLException {
    String thisEnum = "";
    String barcode = null;
    for (Subfield sf : f.subfields) {
      switch (sf.code) {
      case 'p': barcode = sf.value; break;
      case '3': thisEnum = sf.value; break;
      }
    }
    if (barcode == null) return null;

    // lookup item main item this is bound into
    Item masterItem = Items.retrieveItemByBarcode(voyager, barcode);
    if (masterItem == null) return null;
    Integer masterBibId = null;
    String masterTitle = null;
    try (PreparedStatement pstmt = solrInventory.prepareStatement(
        "SELECT b.bib_id, b.title"
        + " FROM bibRecsSolr as b, mfhdRecsSolr as m"
        + " WHERE b.bib_id = m.bib_id AND m.mfhd_id = ?")) {
      pstmt.setInt(1, masterItem.mfhdId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          masterBibId = rs.getInt(1);
          masterTitle = rs.getString(2);
        }
      }
    }
    if (masterBibId == null) return null;
    
    return new BoundWith(masterItem.itemId, masterBibId, masterTitle, concatEnum(masterItem),
        thisEnum, barcode, masterItem.status);
  }

  public BoundWith(int masterItemId, int masterBibId, String masterTitle,
      String masterEnum, String thisEnum, String barcode, ItemStatus status) {
    this.masterItemId = masterItemId;
    this.masterBibId = masterBibId;
    this.masterTitle = masterTitle;
    this.masterEnum = masterEnum;
    this.thisEnum = thisEnum;
    this.barcode = barcode;
    this.status = status;
  }

  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  private static String concatEnum(Item i) {
    List<String> enumchronyear = new ArrayList<>();
    if (i.enumeration != null && !i.enumeration.isEmpty()) enumchronyear.add(i.enumeration);
    if (i.chron != null && !i.chron.isEmpty()) enumchronyear.add(i.chron);
    if (i.year != null && !i.year.isEmpty()) enumchronyear.add(i.year);
    if (enumchronyear.isEmpty())
      return null;
    return String.join(" ", enumchronyear);
  }

  static ObjectMapper mapper = new ObjectMapper();
}
