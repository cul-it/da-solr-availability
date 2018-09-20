package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OpenOrder {

  public String note = null;
  public Integer mfhdId = null;

  public OpenOrder (Connection voyager, Integer bibId) throws SQLException {
    final String getOrderInfoQuery =
        "SELECT line_item.quantity,"+
        "       purchase_order.po_approve_date,"+
        "       line_item_copy_status.mfhd_id,"+
        "       line_item_copy_status.line_item_status"+
        "  FROM line_item, line_item_copy_status, purchase_order"+
        " WHERE line_item.bib_id = ?"+
        "   AND line_item.line_item_id = line_item_copy_status.line_item_id"+
        "   AND line_item_copy_status.line_item_status IN (7, 8)"+ // i.e. (cancelled, approved), neither received
        "   AND line_item.po_id = purchase_order.po_id"+
        "   AND purchase_order.po_type = 1"; // firm order (not ongoing)
    try (PreparedStatement pstmt = voyager.prepareStatement(getOrderInfoQuery)) {
      pstmt.setInt(1, bibId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          switch (rs.getInt("line_item_status")) {
          case 7:
            this.note = "Order cancelled"; break;
          case 8:
            this.note = rs.getInt("quantity")+" Copy Ordered as of "+rs.getString("po_approve_date").substring(0,10);
          }
          this.mfhdId = rs.getInt("mfhd_id");
        }
      }
    }
  }
}
