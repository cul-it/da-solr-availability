package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.cornell.library.integration.voyager.ItemStatuses.StatusCode;
import edu.cornell.library.integration.voyager.ItemTypes.ItemType;

public class ItemStatus {
  public Boolean available;
  public final Map<Integer,String> code;
  public final Boolean shortLoan;
  public final Long due;
  public final Long date;

  @JsonCreator
  public ItemStatus(
      @JsonProperty("available") Boolean available,
      @JsonProperty("code")      Map<Integer,String> code,
      @JsonProperty("due")       Long due,
      @JsonProperty("date")      Long date,
      @JsonProperty("shortLoan") Boolean shortLoan) {
    this.available = available;
    this.code = code;
    this.shortLoan = shortLoan;
    this.due = due;
    this.date = date;
  }

  public ItemStatus( Connection voyager, int item_id, ItemType type ) throws SQLException {
    
    String statusQ = "SELECT * FROM item_status WHERE item_id = ?";
    String circQ = "SELECT current_due_date FROM circ_transactions WHERE item_id = ?";
    boolean foundUnavailable = false;
    Set<StatusCode> statuses = new TreeSet<>();
    Timestamp statusModDate = null;
    Timestamp checkoutDate = null;
    try ( PreparedStatement pstmt = voyager.prepareStatement(statusQ)) {
      pstmt.setInt(1, item_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          int status_id = rs.getInt("item_status");
          StatusCode code = ItemStatuses.getStatusByCode(voyager, status_id);
          statuses.add(code);
          if (statusModDate == null ||
        		  ( rs.getTimestamp("item_status_date") != null && statusModDate.before(rs.getTimestamp("item_status_date"))))
            statusModDate = rs.getTimestamp("item_status_date");
          if ((code.name.equals("Charged") || code.name.equals("Renewed")) &&
              (checkoutDate == null ||
               ( rs.getTimestamp("item_status_date") != null && checkoutDate.before(rs.getTimestamp("item_status_date")))))
            checkoutDate = rs.getTimestamp("item_status_date");
          if (ItemStatuses.getIsUnavailable(status_id))
            foundUnavailable = true;
        }
      }
    }
    if (foundUnavailable)
      this.available = false;
    else if (! statuses.isEmpty() && Arrays.asList(1,11).contains(statuses.iterator().next().id))
      this.available = true;
    else
      this.available = false;
    if (! statuses.isEmpty()) {
      StatusCode c = statuses.iterator().next();
      this.code = new HashMap<>();
      this.code.put(c.id, c.name);
    } else
      this.code = null;
    Long dueDate = null;
    boolean shortLoan = checkoutDate != null && ItemTypes.isShortLoanType(type);
    try ( PreparedStatement pstmt = voyager.prepareStatement(circQ)) {
      pstmt.setInt(1, item_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          Timestamp tmp = rs.getTimestamp(1);
          if (tmp != null) {
            dueDate = tmp.getTime() / 1000 ;
            if (checkoutDate != null) {
              long loanDuration = tmp.getTime() - checkoutDate.getTime();
              if (loanDuration < 86_400_000L ) // 24 hours, in milliseconds
                shortLoan = true;
            }
          }
        }
/*      ResultSetMetaData rsmd = rs.getMetaData();
        for (int i=1; i <= rsmd.getColumnCount() ; i++) {
          String colname = rsmd.getColumnName(i).toLowerCase();
          System.out.println(colname+" : "+rs.getString(i));
        } */
      }
    }
    this.due = dueDate;
    this.shortLoan = (shortLoan)?true:null;
    this.date = (statusModDate == null)?null:statusModDate.getTime()/1000;
  }

}
