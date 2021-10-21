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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Holdings.HoldingSet;

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
                  Map<String,String> olMetadata = (Map<String,String>)orderLine.get("metadata");
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

  private static List<String> orderStatuses = Arrays.asList(
      "Pending","Received Complete","Backordered","Returned","Claimed",
      "Invoice Pending","Invoiced","Canceled","Approved","Received Partial","Rolled Over");
  private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME;

}
