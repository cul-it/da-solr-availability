package edu.cornell.library.integration.folio;

import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.cornell.library.integration.folio.LoanTypes.ExpectedLoanType;
import edu.cornell.library.integration.folio.LoanTypes.LoanType;
import edu.cornell.library.integration.folio.Locations.Location;

public class ItemStatus {
  public String status;
  public Long due = null;
  public Long date = null;
  public Long returned = null;
  public Long returnedUntil = null;

  @JsonCreator
  public ItemStatus(
      @JsonProperty("status")        String status,
      @JsonProperty("due")           Long due,
      @JsonProperty("date")          Long date,
      @JsonProperty("returned")      Long returnedDate,
      @JsonProperty("returnedUntil") Long returnedUntil) {
    this.status = status;
    this.due = due;
    this.date = date;
    this.returned = returnedDate;
    this.returnedUntil = returnedUntil;
  }

  public ItemStatus( OkapiClient okapi, String status, LoanType loanType, Location location ) {

    this.status = status;

    Timestamp statusModDate = null;
    Timestamp checkoutDate = null;

    // nocirc items at the annex are unavailable regardless of the item status
    // DISCOVERYACCESS-4881/DISCOVERYACCESS-4917
    if (this.status.equals("Available")
        && loanType.name.equals(ExpectedLoanType.NOCIRC.toString()) && location.name.equals("Library Annex")) {
      this.status = "Unavailable";
      return;
    }

    Long dueDate = null;
    this.due = dueDate;
  }

}
