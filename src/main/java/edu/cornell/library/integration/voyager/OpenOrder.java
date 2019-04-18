package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.availability.RecordsToSolr.Change;

public class OpenOrder {

  String note = null;
  Integer mfhdId = null;

  OpenOrder (Connection voyager, Integer bibId) throws SQLException {
    final String getOrderInfoQuery =
        "SELECT line_item.quantity,"+
        "       purchase_order.po_approve_date,"+
        "       line_item_copy_status.mfhd_id,"+
        "       line_item_copy_status.line_item_status,"+
        "       line_item_copy_status.status_date"+
        "  FROM line_item, line_item_copy_status, purchase_order"+
        " WHERE line_item.bib_id = ?"+
        "   AND line_item.line_item_id = line_item_copy_status.line_item_id"+
        "   AND line_item_copy_status.line_item_status IN (0, 7, 8)"+ // i.e. (pending, cancelled, approved), not received
        "   AND line_item.po_id = purchase_order.po_id"+
        "   AND purchase_order.po_type = 1"+ // firm order (not ongoing)
        " ORDER BY line_item_copy_status.status_date DESC"+
        " FETCH FIRST 1 ROWS ONLY";
    try (PreparedStatement pstmt = voyager.prepareStatement(getOrderInfoQuery)) {
      pstmt.setInt(1, bibId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          this.mfhdId = rs.getInt("mfhd_id");
          switch (rs.getInt("line_item_status")) {
          case 0:
            this.note = "In pre-order processing";
            Timestamp statusDate = rs.getTimestamp("status_date");
            if ( statusDate != null )
              note += " as of "+format.format(statusDate);
            break;
          case 7:
            this.note = "Order cancelled"; break;
          case 8:
            int quantity = rs.getInt("quantity");
            this.note = String.format("%d cop%s ordered as of %s",
                quantity,(quantity==1)?"y":"ies",format.format(rs.getTimestamp("po_approve_date")));
            break;
          }
          System.out.println(this.note);
        }
      }
    }
  }

  private SimpleDateFormat format = new SimpleDateFormat("M/d/yy");

  public static void detectOrderStatusChanges(
      Connection voyager, Timestamp since, Map<Integer, Set<Change>> changes) throws SQLException {

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
      pstmt.setTimestamp(1,Timestamp.valueOf("2019-04-01 00:00:00"));
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
  }

  private static List<String> orderStatuses = Arrays.asList(
      "Pending","Received Complete","Backordered","Returned","Claimed",
      "Invoice Pending","Invoiced","Canceled","Approved","Received Partial","Rolled Over");
}
