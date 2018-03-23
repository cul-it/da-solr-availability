package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OpenOrder {

  public static String getOrderNote( Connection voyager, Integer bibId ) throws SQLException {
    final String getOrderInfoQuery =
        "SELECT line_item.quantity, purchase_order.po_approve_date, line_item_copy_status.line_item_status"+
        "  FROM line_item, line_item_copy_status, purchase_order"+
        " WHERE line_item.bib_id = ?"+
        "   AND line_item.line_item_id = line_item_copy_status.line_item_id"+
        "   AND line_item_copy_status.line_item_status IN (0,8)"+
        "   AND line_item.po_id = purchase_order.po_id";
    try (PreparedStatement pstmt = voyager.prepareStatement(getOrderInfoQuery)) {
      pstmt.setInt(1, bibId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          return rs.getInt(1)+" Copy Ordered as of "+rs.getString(2).substring(0,10);
      }
    }
    return null;
  }

}
