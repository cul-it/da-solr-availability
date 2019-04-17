package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

class OpenOrder {

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
        "   AND purchase_order.po_type = 1"; // firm order (not ongoing)
    try (PreparedStatement pstmt = voyager.prepareStatement(getOrderInfoQuery)) {
      pstmt.setInt(1, bibId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          switch (rs.getInt("line_item_status")) {
          case 0:
            this.note = "In pre-order processing";
            Timestamp statusDate = rs.getTimestamp("status_date");
            if ( statusDate != null )
              note += " as of "+format.format(statusDate); break;
          case 7:
            this.note = "Order cancelled"; break;
          case 8:
            int quantity = rs.getInt("quantity");
            this.note = String.format("%d cop%s ordered as of %s",
                quantity,(quantity==1)?"y":"ies",format.format(rs.getTimestamp("po_approve_date")));
          }
          this.mfhdId = rs.getInt("mfhd_id");
        }
      }
    }
  }

  private SimpleDateFormat format = new SimpleDateFormat("M/d/yy");
}
