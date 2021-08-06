package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.changes.ChangeDetector;

public class OpenOrder implements ChangeDetector {

  Map<Integer,String> notes = new HashMap<>();

  OpenOrder (Connection voyager, Integer bibId) throws SQLException {
    final String getOrderInfoQuery =
        "SELECT DISTINCT "+
        "       line_item.quantity,"+
        "       purchase_order.po_approve_date,"+
        "       line_item_copy_status.mfhd_id,"+
        "       line_item_copy_status.line_item_status,"+
        "       line_item_copy_status.status_date"+
        "  FROM line_item, line_item_copy_status, purchase_order"+
        " WHERE line_item.bib_id = ?"+
        "   AND line_item.line_item_id = line_item_copy_status.line_item_id"+
        "   AND line_item_copy_status.line_item_status IN (0, 4, 7, 8)"+ // i.e. (pending, claimed, cancelled, approved), not received
        "   AND line_item.po_id = purchase_order.po_id"+
        "   AND purchase_order.po_type = 1"+ // firm order (not ongoing)
        " ORDER BY line_item_copy_status.status_date DESC";
    try (PreparedStatement pstmt = voyager.prepareStatement(getOrderInfoQuery)) {
      pstmt.setInt(1, bibId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          int mfhdId = rs.getInt("mfhd_id");
          String note = null;
          switch (rs.getInt("line_item_status")) {
          case 0:
            note = "In pre-order processing";
            Timestamp statusDate = rs.getTimestamp("status_date");
            if ( statusDate != null )
              note += " as of "+this.format.format(statusDate);
            break;
          case 7:
            if ( this.notes.containsKey(mfhdId) ) continue;
            note = "Order cancelled"; break;
          case 8: case 4:
            int quantity = rs.getInt("quantity");
            if ( quantity == 1 )
              note = String.format("On order as of %s", this.format.format(rs.getTimestamp("po_approve_date")));
            else
              note = String.format("%d copies ordered as of %s",
                  quantity, this.format.format(rs.getTimestamp("po_approve_date")));
            break;
          }
          if (note != null)
            if ( this.notes.containsKey(mfhdId) )
              this.notes.put( mfhdId, note+", "+this.notes.get(mfhdId) );
            else
              this.notes.put( mfhdId, note);
        }
      }
    }
  }

  private SimpleDateFormat format = new SimpleDateFormat("M/d/yy");

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
          Change c = new Change(Change.Type.ORDER,rs.getString("line_item_id"),
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
