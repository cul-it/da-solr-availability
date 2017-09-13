package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
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
  @JsonProperty("status")       public final ItemStatus status;

  public static BoundWith from876Field( Connection voyager, DataField f )
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
    try (PreparedStatement pstmt = voyager.prepareStatement(
        "SELECT bt.bib_id, bt.title_brief"
        + " FROM bib_mfhd bm, bib_text bt"
        + " WHERE bt.bib_id = bm.bib_id AND bm.mfhd_id = ?")) {
      pstmt.setInt(1, masterItem.mfhdId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          masterBibId = rs.getInt("BIB_ID");
          masterTitle = rs.getString("TITLE_BRIEF").trim();
        }
      }
    }
    if (masterBibId == null) return null;
    
    return new BoundWith(masterItem.itemId, masterBibId, masterTitle,
        masterItem.enumeration, thisEnum, masterItem.status);
  }

  public BoundWith(int masterItemId, int masterBibId, String masterTitle,
      String masterEnum, String thisEnum, ItemStatus status) {
    this.masterItemId = masterItemId;
    this.masterBibId = masterBibId;
    this.masterTitle = masterTitle;
    this.masterEnum = masterEnum;
    this.thisEnum = thisEnum;
    this.status = status;
  }

  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

}
