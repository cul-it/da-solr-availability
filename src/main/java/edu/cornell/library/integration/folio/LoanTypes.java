package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LoanTypes {

  public static void initialize( OkapiClient okapi ) throws IOException { if (_byUuid.isEmpty()) populateLoanTypes(okapi); }

  public final static LoanType getByUuid( String uuid ) {
    if (_byUuid.containsKey(uuid)) return _byUuid.get(uuid);
    return null;
  }
  public final static LoanType getByName( String name ) {
    if (_byName.containsKey(name)) return _byName.get(name);
    return null;
  }
  public final static LoanType byExpectedType( ExpectedLoanType type ) {
    return _byName.get(type.name());
  }

  public enum ExpectedLoanType{
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
    RES   ("Reserves"),
    ILL   ("ILL LOAN");

    final private String label;
    private ExpectedLoanType(String label) { this.label = label; }
    @Override public String toString() { return this.label; }
    static ExpectedLoanType byName(String name) {
      for (ExpectedLoanType exp : ExpectedLoanType.values() )
        if ( exp.label.equals(name) ) return exp;
      return null;
    }
  }

  public final static EnumSet<ExpectedLoanType> shortLoanTypes = EnumSet.of(
      ExpectedLoanType.DAY1,
      ExpectedLoanType.HOUR12,
      ExpectedLoanType.HOUR2,
      ExpectedLoanType.HOUR3,
      ExpectedLoanType.HOUR4,
      ExpectedLoanType.HOUR5,
      ExpectedLoanType.HOUR8,
      ExpectedLoanType.RES);

  private static final Map<String,LoanType> _byUuid = new HashMap<>();
  private static final Map<String,LoanType> _byName = new HashMap<>();

  public static class LoanType {
    @JsonProperty("id")   public final String uuid;
    @JsonProperty("name") public final String name;
    @JsonIgnore           public final boolean shortLoan;

    public LoanType(String uuid, String name, boolean shortLoan) {
      this.uuid = uuid;
      this.name = name;
      this.shortLoan = shortLoan;
    }

    public LoanType(@JsonProperty("id") String uuid,@JsonProperty("name") String name) {
      this.uuid = uuid;
      this.name = name;
      this.shortLoan = false;
    }
  }

  private static void populateLoanTypes( OkapiClient okapi ) throws IOException {
    List<Map<String,Object>> okapiTypes = okapi.queryAsList("/loan-types",null,500 );
    EnumSet<ExpectedLoanType> expected = EnumSet.allOf(ExpectedLoanType.class);
    for ( Map<String,Object> okapiType : okapiTypes ) {
      String id = (String)okapiType.get("id");
      String name = (String)okapiType.get("name");
      ExpectedLoanType exp = ExpectedLoanType.byName(name);
      if ( exp == null ) {
        System.out.printf("Unexpected loan type (%s): %s\n",id,name);
        System.exit(1);
      }
      expected.remove(exp);
      boolean shortLoan = shortLoanTypes.contains(exp);
      _byUuid.put(id,  new LoanType(id,name,shortLoan));
      _byName.put(name,new LoanType(id,name,shortLoan));
    }
    
  }

}
