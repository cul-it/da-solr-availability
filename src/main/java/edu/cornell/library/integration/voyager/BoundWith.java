package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;

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

  public static EnumSet<Flag> dedupeBoundWithReferences(HoldingSet holdings, ItemList items) {
    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    for (Integer holdingId : holdings.getMfhdIds()) {
      Holding h = holdings.get(holdingId);
      Set<Item> i = items.getItems().get(holdingId);
      int emptyItemCount = countEmptyItems( i );
      int holdingRefCount = countHoldingReferences( h );
      if ( emptyItemCount > 0 ) {
        flags.add(Flag.EMPTY_ITEMS);
        if ( holdingRefCount > 0 ) {
          flags.add(Flag.HOLDING_REFS);
          if ( emptyItemCount != holdingRefCount ) {
            flags.add(Flag.NOT_DEDUPED);
          } else {
            actuallyDedupe(h,i,emptyItemCount,flags);
            flags.add(Flag.DEDUPED);
          }
        }
      } 
      else if ( holdingRefCount > 0 ) flags.add(Flag.HOLDING_REFS);
    }
    return flags;
  }

  @JsonCreator
  public BoundWith(
      @JsonProperty("masterItemId") int masterItemId,
      @JsonProperty("masterBibId")  int masterBibId,
      @JsonProperty("masterTitle")  String masterTitle,
      @JsonProperty("masterEnum")   String masterEnum,
      @JsonProperty("thisEnum")     String thisEnum,
      @JsonProperty("status")       ItemStatus status) {
    this.masterItemId = masterItemId;
    this.masterBibId = masterBibId;
    this.masterTitle = masterTitle;
    this.masterEnum = masterEnum;
    this.thisEnum = thisEnum;
    this.status = status;
  }

  public enum Flag{
    EMPTY_ITEMS ("Bound With: Empty Items"),
    HOLDING_REFS("Bound With: Holdings Reference"),
    DEDUPED     ("Bound With: Deduped"),
    NOT_DEDUPED ("Bound With: Not Deduped"),
    MULTI_BW    ("Bound With: Multiple, Matching Counts"),
    REF_STATUS  ("Bound With: Item Status From Holdings Ref");

    private String availFlag;
    private Flag( String s ) { availFlag = s; }
    public String getAvailabilityFlag() { return availFlag; }

  }
  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  private static void actuallyDedupe(Holding holding, Set<Item> items, int count, EnumSet<Flag> flags) {
    if (count > 1) {
      flags.add(Flag.MULTI_BW);
    } else {
      ItemStatus refStatus = holding.boundWiths.values().iterator().next().status;
      for (Item i : items)
        if (i.empty)
          if ( ! refStatus.matches(i.status) && refStatus.newerThan(i.status) ) {
            i.status = refStatus;
            flags.add(Flag.REF_STATUS);
          }
    }
    holding.boundWiths = null;
  }

  private static int countEmptyItems(Set<Item> items) {
    int count = 0;
    for (Item i : items) if (i.empty) count++;
    return count;
  }

  private static int countHoldingReferences(Holding h) {
    if (h.boundWiths == null) return 0;
    return h.boundWiths.size();
  }
}
