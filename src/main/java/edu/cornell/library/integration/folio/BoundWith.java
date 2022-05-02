package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.Items.ItemList;

public class BoundWith {

  @JsonProperty("masterItemId") public final String masterItemId;
  @JsonProperty("masterBibId")  public final String masterBibId;
  @JsonProperty("masterTitle")  public final String masterTitle;
  @JsonProperty("masterEnum")   public final String masterEnum;
  @JsonProperty("thisEnum")     public final String thisEnum;
  @JsonProperty("barcode")      public final String barcode;
  @JsonProperty("status")       public final ItemStatus status;

  private static Pattern barcodeP = Pattern.compile("^(31924\\d*)(.*)$");
  public static Map<String,BoundWith> fromNote( Connection inventory, String note, Holding holding )
      throws SQLException, JsonParseException, JsonMappingException, IOException {

    String text = note.trim();
    int barcodePos = text.indexOf("31924");
    Map<String,BoundWith> b = new HashMap<>();
    while ( barcodePos > -1 ) {
      String thisEnum = null;
      if ( barcodePos > 0 ) {
        thisEnum = text.substring(0,barcodePos).replaceAll("[\\|\\\\n]", "").trim();
        text = text.substring(barcodePos);
      }
      Matcher m = barcodeP.matcher(text);
      String barcode = null;
      if ( m.matches() ) {
        barcode = m.group(1);
        text = m.group(2).trim();
      }
      
      if (barcode == null) break;
      if (text.length() > barcode.length())
        text = text.substring(barcode.length()).trim();
      else text = "";
      barcodePos = text.indexOf("31924");

      // lookup item main item this is bound into
      Item masterItem = Items.retrieveItemByBarcode(inventory, barcode, holding);
      if (masterItem == null) continue;
      String masterBibId = null;
      String masterTitle = null;
      try (PreparedStatement pstmt = inventory.prepareStatement(
          "SELECT brs.bib_id, brs.title"
          + " FROM itemFolio i, holdingFolio h, bibRecsSolr brs"
          + " WHERE i.hrid = ? AND i.holdingHrid = h.hrid AND h.instanceHrid = brs.bib_id")) {
        pstmt.setString(1, masterItem.hrid);
        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            masterBibId = rs.getString("bib_id");
            masterTitle = rs.getString("title").trim();
          }
        }
      }
      if (masterBibId == null) continue;
      b.put(masterItem.hrid, new BoundWith(masterItem.id, masterBibId, masterTitle,
        masterItem.enumeration, thisEnum, barcode, masterItem.status));
    }

    return b;

  }

  public static EnumSet<Flag> dedupeBoundWithReferences(HoldingSet holdings, ItemList items) {
    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    for (String holdingId : holdings.getUuids()) {
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
      @JsonProperty("masterItemId") String masterItemId,
      @JsonProperty("masterBibId")  String masterBibId,
      @JsonProperty("masterTitle")  String masterTitle,
      @JsonProperty("masterEnum")   String masterEnum,
      @JsonProperty("thisEnum")     String thisEnum,
      @JsonProperty("barcode")      String barcode,
      @JsonProperty("status")       ItemStatus status2) {
    this.masterItemId = masterItemId;
    this.masterBibId = masterBibId;
    this.masterTitle = masterTitle;
    this.masterEnum = masterEnum;
    this.thisEnum = thisEnum;
    this.barcode = barcode;
    this.status = status2;
  }

  public enum Flag{
    EMPTY_ITEMS ("Bound With: Empty Items"),
    HOLDING_REFS("Bound With: Holdings Reference"),
    DEDUPED     ("Bound With: Deduped"),
    NOT_DEDUPED ("Bound With: Not Deduped"),
    MULTI_BW    ("Bound With: Multiple, Matching Counts"),
    REF_STATUS  ("Bound With: Item Status From Holdings Ref");

    private String availFlag;
    private Flag( String s ) { this.availFlag = s; }
    public String getAvailabilityFlag() { return this.availFlag; }

  }
  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  private static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  private static void actuallyDedupe(Holding holding, Set<Item> items, int count, EnumSet<Flag> flags) {
    if (count > 1) {
      flags.add(Flag.MULTI_BW);
    } else {
      ItemStatus refStatus = holding.boundWiths.values().iterator().next().status;
      for (Item i : items)
        if (i.empty != null && i.empty)
          if ( ! refStatus.status.equals("Available") && i.status.status.equals("Available") ) {
            i.status = refStatus;
            flags.add(Flag.REF_STATUS);
          }
    }
    holding.boundWiths = null;
  }

  private static int countEmptyItems(Set<Item> items) {
    if ( items == null ) return 0;
    int count = 0;
    for (Item i : items) if (i.empty != null && i.empty) count++;
    return count;
  }

  private static int countHoldingReferences(Holding h) {
    if (h.boundWiths == null) return 0;
    return h.boundWiths.size();
  }


  public static boolean storeRecordLinksInInventory (
      Connection inventory, String bibId, HoldingSet holdings)throws SQLException {

    // Determine whether this bib contains master items into which other bibs are bound
    boolean masterBoundWith = false;
    try ( PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT * FROM boundWith WHERE masterInstanceHrid = ? LIMIT 1")) {
      pstmt.setString(1, bibId);
      try ( ResultSet rs = pstmt.executeQuery() ) {
        if (rs.next()) masterBoundWith = true;
      }
    }
    Map<String,String> previousLinks = new HashMap<>();
    Map<String,String> currentLinks  = new HashMap<>();

    // Pull previous links from inventory
    try ( PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT masterInstanceHrid, masterItemId FROM boundWith WHERE boundWithInstanceHrid = ?")) {
      pstmt.setString(1, bibId);
      try ( ResultSet rs = pstmt.executeQuery() ) {
        while (rs.next()) previousLinks.put(rs.getString(2),rs.getString(1));
      }
    }

    // Collate current links from holdings
    for ( String uuid : holdings.getUuids() ) {
      Holding mfhd = holdings.get(uuid);
      if ( mfhd.boundWiths != null )
        for ( BoundWith bw : mfhd.boundWiths.values() )
          currentLinks.put(bw.masterItemId, bw.masterBibId);
    }

    // We're done if the two lists match
    if ( currentLinks.size() == previousLinks.size()
        && currentLinks.keySet().containsAll(previousLinks.keySet()) )
      return masterBoundWith;

    // One or more link has been dropped
    if ( ! currentLinks.keySet().containsAll(previousLinks.keySet()) )
      try ( PreparedStatement pstmt = inventory.prepareStatement(
          "DELETE FROM boundWith"
          + " WHERE boundWithInstanceHrid = ? AND masterItemId = ? AND masterInstanceHrid = ?") ) {
        pstmt.setString(1, bibId);
        for ( Entry<String, String> e : previousLinks.entrySet() )
          if ( ! currentLinks.containsKey(e.getKey()) ) {
            pstmt.setString(2, e.getKey());
            pstmt.setString(3, e.getValue());
            pstmt.addBatch();
          }
        pstmt.executeBatch();
      }

    // One or more link has been added
    if ( ! previousLinks.keySet().containsAll(currentLinks.keySet()) )
      try (PreparedStatement pstmt = inventory.prepareStatement(
          "INSERT INTO boundWith ( boundWithInstanceHrid, masterItemId, masterInstanceHrid )"
          + " VALUES ( ?, ?, ? )")) {
        pstmt.setString(1, bibId);
        for ( Entry<String,String> e : currentLinks.entrySet() )
          if ( ! previousLinks.containsKey(e.getKey()) ) {
            pstmt.setString(2, e.getKey());
            pstmt.setString(3, e.getValue());
            pstmt.addBatch();
          }
        pstmt.executeBatch();
      }

    return masterBoundWith;
  }

  public static void identifyAndQueueOtherBibsInMasterVolume(
      Connection inventory, String bibId, Set<String> itemIds) throws SQLException {
    Map<String,Set<String>> otherBibs = new HashMap<>();
    try (PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT masterItemId, boundWithInstanceHrid FROM boundWith WHERE masterInstanceHrid = ?")) {
      pstmt.setString(1, bibId);
      try ( ResultSet rs = pstmt.executeQuery() ) {
        while ( rs.next() ) {
          String itemId = rs.getString(1);
          String otherBib = rs.getString(2);
          if ( itemIds.contains(itemId) && ! otherBib.equals( bibId )) {
            if ( ! otherBibs.containsKey(otherBib) )
              otherBibs.put(otherBib, new HashSet<String>() );
            otherBibs.get(otherBib).add(String.valueOf(itemId));
          }
        }
      }
    }
    if ( otherBibs.isEmpty() ) return;
    try (PreparedStatement pstmt = inventory.prepareStatement(
        "INSERT INTO availabilityQueue (hrid, priority, cause, record_date) VALUES (?,3,?,NOW())")) {
      for ( Entry<String,Set<String>> e : otherBibs.entrySet() ) {
        pstmt.setString(1, e.getKey());
        pstmt.setString(2, "Bound with update from b"+bibId+" and i"+String.join(", i", e.getValue()));
        pstmt.addBatch();
      }
      pstmt.executeBatch();
    }
  }
}
