package edu.cornell.library.integration.folio;

import java.util.EnumSet;

public class LoanTypes {

  public enum LoanType{
    DAY1  ("1 Day Loan"),
    HOUR12("12 Hour Loan"),
    DAY14 ("14 Day Loan"),
    DAY2  ("2 Day Loan"),
    HOUR2 ("2 Hour Loan"),
    DAY3  ("3 Day Loan"),
    HOUR3 ("3 Hour Loan"),
    DAY4  ("4 Day Loan"),
    HOUR4 ("4 Hour Loan"),
    HOUR5 ("5 Hour Loan"),
    DAY7  ("7 Day Loan"),
    HOUR8 ("8 Hour Loan"),
    BD    ("BD LOAN"),
    CIRC  ("Circulating"),
    NOCIRC("Non-circulating"),
    RES   ("Reserves");

    final private String label;
    private LoanType(String label) { this.label = label; }
    @Override public String toString() { return this.label; }
  }

  public final static EnumSet<LoanType> shortLoanTypes = EnumSet.of(
      LoanType.DAY1,
      LoanType.HOUR12,
      LoanType.HOUR2,
      LoanType.HOUR3,
      LoanType.HOUR4,
      LoanType.HOUR5,
      LoanType.HOUR8,
      LoanType.RES);
}
