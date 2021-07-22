package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.changes.ChangeDetector;
import edu.cornell.library.integration.folio.Holdings.HoldingSet;

public class OpenOrder implements ChangeDetector {

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
                  Map<String,String> olMetadata = (Map<String,String>)orderLine.get("metadata");
                  Timestamp polCreateDate = Timestamp.from(Instant.parse(
                      olMetadata.get("createdDate").replace("+00:00","Z")));
                  note = "In pre-order processing as of "+format.format(polCreateDate);
                  break;
                case "Cancelled":
                  note = "Order cancelled"; break;
                case "Awaiting Receipt":
                  Timestamp approvalDate = Timestamp.from(Instant.parse(
                      ((String)order.get("approvalDate")).replace("+00:00","Z")));
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
                String locationId = (String)location.get("locationId");
                for ( Holding h : holdings.values() ) {
                  // Block order note if:
                  // * There are any items on the mfhd
                  // * The holding call number is "Available for the Library to Purchase"
                  // * The holding call number is "In Process"
                  if (h.location != null
                      && h.location.id.equals(locationId)
                      && (h.itemSummary == null || h.itemSummary.itemCount == 0)
                      && ! (h.call != null
                            && (h.call.equalsIgnoreCase("Available for the Library to Purchase")
                                || h.call.equalsIgnoreCase("In Process")))) {
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

  private static ObjectMapper mapper = new ObjectMapper();

  /* Object instantiation without arguments is intended only for detectChanges()
   * which, as an override method can't be defined as static, although that would be
   * preferrable.
   */
  public OpenOrder() {}
  @Override
  public Map<Integer, Set<Change>> detectChanges(
      Connection voyager, Timestamp since) throws SQLException {

    Map<Integer,Set<Change>> changes = new HashMap<>();

    final String getRecentOrderStatusChanges =
        "SELECT li.bib_id, lics.line_item_id, lics.status_date, lics.mfhd_id, lics.line_item_status" + 
        "  FROM line_item li, line_item_copy_status lics, bib_master bm, mfhd_master mm" + 
        "  WHERE li.line_item_id = lics.line_item_id" + 
        "    AND lics.status_date > ?" + 
        "    AND li.bib_id = bm.bib_id" + 
        "    AND bm.suppress_in_opac = 'N'" + 
        "    AND lics.mfhd_id = mm.mfhd_id" + 
        "    AND mm.suppress_in_opac = 'N'";

    try (  PreparedStatement pstmt = voyager.prepareStatement(getRecentOrderStatusChanges)   ) {
      pstmt.setTimestamp(1,since);
      try (  ResultSet rs = pstmt.executeQuery()  ) {
        while (rs.next()) {
          Integer bibId = rs.getInt("bib_id");
          Change c = new Change(Change.Type.ORDER,rs.getInt("line_item_id"),
              String.format("Order status update on h%d: %s",
                  rs.getInt("mfhd_id"), orderStatuses.get(rs.getInt("line_item_status"))),
              rs.getTimestamp("status_date"),null);
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
    return changes;

  }

  private static List<String> orderStatuses = Arrays.asList(
      "Pending","Received Complete","Backordered","Returned","Claimed",
      "Invoice Pending","Invoiced","Canceled","Approved","Received Partial","Rolled Over");
}
