package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.LoanTypes.ExpectedLoanType;
import edu.cornell.library.integration.folio.ServicePoints.ServicePoint;

public class ItemStatus {
  public String status;
  public Long due = null;
  public Long returned = null;
  public Boolean shortLoan = null;
  public Long date = null;
  @JsonIgnore  public Instant returnedUntil = null;

  public ItemStatus( Connection inventory, Map<String,Object> rawItem, Item item )
      throws SQLException, IOException {

    Map<String,String> statusData = (Map<String,String>)rawItem.get("status");
    this.status = statusData.get("name");

    if (this.status.equals("Checked out")) {
      List<Map<String, Object>> loans = new ArrayList<>();
//          folio.queryAsList("/loan-storage/loans", "itemId=="+item.id, null);
      try (PreparedStatement loansByItem = inventory.prepareStatement("SELECT * FROM loanFolio WHERE itemHrid = ?")){
        loansByItem.setString(1, item.hrid);
        try ( ResultSet rs = loansByItem.executeQuery() ) {
          while (rs.next()) loans.add(mapper.readValue(rs.getString("content"), Map.class));
        }
        for (Map<String,Object> loan : loans) {
          if ( ! ((Map<String,String>)loan.get("status")).get("name").equals("Open") ) continue;
          if (loan.containsKey("dueDate")) {
            Instant instant = isoDT.parse((String)loan.get("dueDate"),Instant::from);
            this.due = easternOffsetAdjustedEpochSecond(instant);
          }
        }
        if ( item.loanType.shortLoan ) this.shortLoan = true;
      }
      return;
    }

    // nocirc items at the annex are unavailable regardless of the item status
    // DISCOVERYACCESS-4881/DISCOVERYACCESS-4917
    if (this.status.equals("Available")
        && item.loanType != null
        && item.loanType.name.equals(ExpectedLoanType.NOCIRC.toString())
        && item.location != null
        && item.location.name.equals("Library Annex")) {

      this.status = "Unavailable";
      return;

    }

    if ( this.status.equals("Available") ) {
      // TODO check for recent return, delayed queueing for end of returned status
      if (! rawItem.containsKey("lastCheckIn"))
        return;
      Map<String,String> lastCheckIn = (Map<String,String>)rawItem.get("lastCheckIn");
      if ( ! lastCheckIn.containsKey("dateTime") )
        return;
      Instant returned = isoDT.parse(lastCheckIn.get("dateTime"),Instant::from);
      ServicePoint servicePoint = ServicePoints.getByUuid(item.location.primaryServicePoint);
      int lagMinutes = (servicePoint.shelvingLagTime == null)?4320:servicePoint.shelvingLagTime;
      Instant returnedUntil = returned.plusSeconds(lagMinutes*60);
      if ( returnedUntil.isAfter(Instant.now()) ) {
        this.returned = easternOffsetAdjustedEpochSecond(returned);
        this.returnedUntil = returnedUntil;
        if ( item.loanType.shortLoan ) this.shortLoan = true;
      }
      return;
    }

    this.date = getStatusDate(statusData);
  }

  public ItemStatus(
      @JsonProperty("status") String status) {
    this.status = status;
  }

  /*
   * Adjust epoch seconds of given instant with offset between UTC and America/New_York in seconds
   * While Eastern Daylight Time is in effect, the offset will be -14400
   * Otherwise, it will be -18000
   */
  public static long easternOffsetAdjustedEpochSecond(Instant instant) {
    ZoneId nyZone = ZoneId.of("America/New_York");
    ZoneOffset offset = nyZone.getRules().getOffset(instant);
    return instant.getEpochSecond() + offset.getTotalSeconds();
  }

  protected Long getStatusDate(Map<String,String> statusData) {
    if ( statusData.containsKey("date") ) {
      Instant instant = isoDT.parse(statusData.get("date"),Instant::from);
      return easternOffsetAdjustedEpochSecond(instant);
    }
    return null;
  }

  public static ItemStatus AVAIL = new ItemStatus("Available");
  static PreparedStatement loansByItem = null;
  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }
  private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME;
}
