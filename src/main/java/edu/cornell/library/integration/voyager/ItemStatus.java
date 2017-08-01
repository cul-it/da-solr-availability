package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ItemStatus {
  public final boolean available;
  public final Map<Integer,String> codes;
  public final Timestamp current_due_date;

  public ItemStatus( Connection voyager, int item_id ) throws SQLException {
    
    String statusQ = "SELECT * FROM item_status WHERE item_id = ?";
    String circQ = "SELECT current_due_date FROM circ_transactions WHERE item_id = ?";
    boolean foundUnavailable = false;
    Map<Integer,String> statuses = new HashMap<>();
    try ( PreparedStatement pstmt = voyager.prepareStatement(statusQ)) {
      pstmt.setInt(1, item_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          int status_id = rs.getInt("item_status");
          String status_desc = ItemStatuses.getStatusNameById(voyager, status_id);
          statuses.put(status_id, status_desc);
          if (ItemStatuses.getIsUnvailable(status_id))
            foundUnavailable = true;
        }
      }
    }
    if (foundUnavailable)
      this.available = false;
    else if (statuses.containsKey(1) || statuses.containsKey(11))
      this.available = true;
    else
      this.available = false;
    this.codes = Collections.unmodifiableMap(statuses);
    Timestamp dueDate = null;
    try ( PreparedStatement pstmt = voyager.prepareStatement(circQ)) {
      pstmt.setInt(1, item_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          dueDate = rs.getTimestamp(1);
//        ResultSetMetaData rsmd = rs.getMetaData();
//        for (int i=1; i <= rsmd.getColumnCount() ; i++) {
//          String colname = rsmd.getColumnName(i).toLowerCase();
//          System.out.println(colname);
//        }
      }
    }
    this.current_due_date = dueDate;
  }

}
