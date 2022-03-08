package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;
import edu.cornell.library.integration.folio.Locations.Location;

public class OpenOrder {

  Map<Integer,String> notes = new HashMap<>();

  public static boolean applyOpenOrders( Connection inventory, HoldingSet holdings, String bib)
      throws SQLException, IOException {
    boolean onOrder = false;

    SimpleDateFormat format = new SimpleDateFormat("M/d/yy");

    try ( PreparedStatement ol_st = inventory.prepareStatement(
        "SELECT * FROM orderLineFolio WHERE instanceHrid = ?");
        PreparedStatement o_st = inventory.prepareStatement(
            "SELECT * FROM orderFolio WHERE id = ?")) {
      ol_st.setString(1, bib);
      try (ResultSet ol_rs = ol_st.executeQuery()){
        OL: while ( ol_rs.next()) {
          Map<String,Object> orderLine = mapper.readValue(ol_rs.getString("content"),Map.class);
          String receiptStatus = (String)orderLine.get("receiptStatus");
          if ( receiptStatus.equals("Ongoing")
              || receiptStatus.equals("Fully Received")
              || receiptStatus.equals("Partially Received")
              || receiptStatus.equals("Receipt Not Required"))
            continue OL;
          o_st.setString(1, (String)orderLine.get("purchaseOrderId"));
          try ( ResultSet o_rs = o_st.executeQuery() ) {
            O: while (o_rs.next()) {
              Map<String,Object> order = mapper.readValue(o_rs.getString("content"),Map.class);
              if ( ((String)order.get("orderType")).equals("Ongoing")) continue O;
              if ( ! orderLine.containsKey("locations") ) continue O;
              for ( Map<String,Object> location : (List<Map<String,Object>>)orderLine.get("locations") ) {
                String note = null;
                switch (receiptStatus) {
                case "Pending":
                  Map<String,String> olMetadata = Map.class.cast(orderLine.get("metadata"));
                  Timestamp polCreateDate = Timestamp.from(
                      isoDT.parse((olMetadata.get("createdDate")),Instant::from));
                  note = "In pre-order processing as of "+format.format(polCreateDate);
                  break;
                case "Cancelled":
                  note = "Order cancelled"; break;
                case "Awaiting Receipt":
                  Timestamp approvalDate = Timestamp.from(
                      isoDT.parse(((String)order.get("approvalDate")),Instant::from));
                  int quantity = (int)location.get("quantity");
                  if ( quantity == 1 )
                    note = "On order as of "+format.format(approvalDate);
                  else
                    note = String.format(
                        "%d copies ordered as of %s", quantity, format.format(approvalDate));
                  break;
                default:
                  System.out.println("Unexpected pol receipt status: "+receiptStatus);
                  continue OL;
                }
                // Block order note if:
                // * There are any items on the mfhd
                // * The holding call number is "Available for the Library to Purchase"
                // * The holding call number is "In Process"
                String orderHoldId = (String)location.get("holdingId");
                if ( orderHoldId != null ) {
                  Holding h = holdings.get(orderHoldId);
                  if ( h != null && ! hasExtantItem(h) && ! hasBlockedCallNumber( h.call )) {
                    h.orderNote = note;
                    onOrder = true;
                  }
                }
                String orderLocId = (String)location.get("locationId");
                if ( orderLocId != null ) for (Holding h : holdings.values() ) {
                  if (hasMatchingLocation(h.location,orderLocId)
                      && ! hasExtantItem(h) && ! hasBlockedCallNumber(h.call)) {
                    h.orderNote = note;
                    onOrder = true;
                  }
                }
              }
            }
          }
        }
      }
    }

    return onOrder;
  }

  private static boolean hasMatchingLocation( Location holdingLoc, String orderLocationUuid ) {
    if ( holdingLoc == null ) return false;
    return holdingLoc.id.equals(orderLocationUuid);
  }

  private static boolean hasBlockedCallNumber( String call ) {
    if (call == null) return false;
    if ( call.equalsIgnoreCase("Available for the Library to Purchase") ) return true;
    if ( call.equalsIgnoreCase("In Process") ) return true;
    return false;
  }

  private static boolean hasExtantItem( Holding h ) {
    if (h.itemSummary == null) return false;
    if (h.itemSummary.itemCount == 0) return false;
    int onOrderItemCount = 0;
    if ( h.itemSummary.unavail != null )
      for ( ItemReference i : h.itemSummary.unavail )
        if ( i.status.status.contains("rder") ) //On order, Order closed
          onOrderItemCount++;
    return h.itemSummary.itemCount > onOrderItemCount;
  }

  private static ObjectMapper mapper = new ObjectMapper();

  /* Object instantiation without arguments is intended only for detectChanges()
   * which, as an override method can't be defined as static, although that would be
   * preferrable.
   */
  public OpenOrder() {}

  private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME;

}
