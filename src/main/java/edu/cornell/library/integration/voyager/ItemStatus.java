package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.cornell.library.integration.voyager.ItemStatuses.StatusCode;
import edu.cornell.library.integration.voyager.ItemTypes.ItemType;
import edu.cornell.library.integration.voyager.Locations.Location;

public class ItemStatus {
  public Boolean available;
  public Map<Integer,String> code;
  public final Boolean shortLoan;
  public final Long due;
  public Long date;
  @JsonIgnore
  public final Set<StatusCode> statuses;

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
    this.statuses = null;
  }

  public ItemStatus( Connection voyager, int item_id, ItemType type, Location location ) throws SQLException {
    
    String statusQ = "SELECT * FROM item_status WHERE item_id = ?";
    String circQ = "SELECT current_due_date FROM circ_transactions WHERE item_id = ?";
    boolean foundUnavailable = false;
    this.statuses = new TreeSet<>();
    Timestamp statusModDate = null;
    Timestamp checkoutDate = null;
    try ( PreparedStatement pstmt = voyager.prepareStatement(statusQ)) {
      pstmt.setInt(1, item_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          int status_id = rs.getInt("item_status");
          StatusCode code = ItemStatuses.getStatusByCode(voyager, status_id);
          this.statuses.add(code);
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
    else if (! this.statuses.isEmpty() && Arrays.asList(1,11).contains(this.statuses.iterator().next().id))
      this.available = true;
    else
      this.available = false;

    if (! this.statuses.isEmpty()) {
      StatusCode c = this.statuses.iterator().next();
      this.code = new HashMap<>();
      this.code.put(c.id, c.name);

      // nocirc items at the annex are unavailable regardless of the item status
      // DISCOVERYACCESS-4881/DISCOVERYACCESS-4917
      if (this.available && type.name.equals("nocirc") && location.name.equals("Library Annex")) {
        this.available = false;
        this.code = FAKE_UNAVAIL_STATUS;
      }

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
    this.date = (statusModDate == null || this.code == null || this.code.containsKey(FAKE_UNAVAIL_STATUS_CODE))?
        null:statusModDate.getTime()/1000;
  }

  public boolean matches( ItemStatus other ) {
    if ( this.available != null 
        && ! this.available.equals( other.available ) ) return false;
    if ( this.code != null 
        && ! this.code.equals(other.code) ) return false;
    if ( this.due != null
        && ! this.due.equals(other.due) ) return false;
    return true;
  }

  public boolean newerThan( ItemStatus other ) {
    if ( this.date == null || other.date == null ) return false;
    return ( this.date > other.date );
  }

  private static final Map<Integer,String> FAKE_UNAVAIL_STATUS;
  private static final int FAKE_UNAVAIL_STATUS_CODE = 31;
  static {
    Map<Integer,String> t = new HashMap<>();
    t.put(FAKE_UNAVAIL_STATUS_CODE, "Unavailable");
    FAKE_UNAVAIL_STATUS = Collections.unmodifiableMap(t);
  }
}
