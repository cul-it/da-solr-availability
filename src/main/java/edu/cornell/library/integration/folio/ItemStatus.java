package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
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
//          okapi.queryAsList("/loan-storage/loans", "itemId=="+item.id, null);
      if ( loansByItem == null )
        loansByItem = inventory.prepareStatement("SELECT * FROM loanFolio WHERE itemHrid = ?");
      loansByItem.setString(1, item.hrid);
      try ( ResultSet rs = loansByItem.executeQuery() ) {
        while (rs.next()) loans.add(mapper.readValue(rs.getString("content"), Map.class));
      }
      for (Map<String,Object> loan : loans) {
        if ( ! ((Map<String,String>)loan.get("status")).get("name").equals("Open") ) continue;
        this.due = Instant.parse(((String)loan.get("dueDate")).replace("+00:00","Z")).getEpochSecond();
      }
      if ( item.loanType.shortLoan ) this.shortLoan = true;
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
      Instant returned = Instant.parse(lastCheckIn.get("dateTime").replace("+00:00","Z"));
      ServicePoint servicePoint = ServicePoints.getByUuid(item.location.primaryServicePoint);
      int lagMinutes = (servicePoint.shelvingLagTime == null)?4320:servicePoint.shelvingLagTime;
      Instant returnedUntil = returned.plusSeconds(lagMinutes*60);
      if ( returnedUntil.isAfter(Instant.now()) ) {
        this.returned = returned.getEpochSecond();
        this.returnedUntil = returnedUntil;
        if ( item.loanType.shortLoan ) this.shortLoan = true;
      }
      return;
    }

    if ( statusData.containsKey("date") )
      this.date = Instant.parse(statusData.get("date").replace("+00:00","Z")).getEpochSecond();
  }
  public ItemStatus(String status) { this.status = status; }

  public static ItemStatus AVAIL = new ItemStatus("Available");
  static PreparedStatement loansByItem = null;
  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }
}
