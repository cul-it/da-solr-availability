package edu.cornell.library.integration.folio;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Change implements Comparable<Change>{
  public Type type;
  private final String recordId;
  public final String detail;
  private final Timestamp changeDate;
  private final String location;

  public Timestamp date() { return this.changeDate; }

  public Change (Type type, String id, String detail, Timestamp changeDate, String location) {
    this.type = type;
    this.recordId = id;
    this.detail = detail;
    this.changeDate = changeDate;
    this.location = location;
  }


  public static Timestamp getCurrentToDate( Connection inventory, String key ) throws SQLException {

    try (PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT current_to_date FROM updateCursor WHERE cursor_name = ?")) {
      pstmt.setString(1, key);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          return rs.getTimestamp(1);
      }
      
    }
    return null;
  }

  public static void setCurrentToDate(Timestamp currentTo, Connection inventory, String key ) throws SQLException {
    
    try (PreparedStatement pstmt = inventory.prepareStatement(
        "REPLACE INTO updateCursor ( cursor_name, current_to_date ) VALUES (?,?)")) {
      pstmt.setString(1, key);
      pstmt.setTimestamp(2, currentTo);
      pstmt.executeUpdate();
    }
  }

  @Override
  public String toString() {
    return this.toString(true);
  }

  private String toString(boolean showAgeOfChange) {
    StringBuilder sb = new StringBuilder();
    sb.append(this.type.name());
    if (this.recordId != null)
      sb.append(" ").append(this.recordId);
    if (this.location != null)
      sb.append(" ").append(this.location);
    if (this.detail != null)
      sb.append(" ").append(this.detail);
    if (this.changeDate != null) {
      sb.append(" ").append(String.format("%1$TD %1$TT",this.changeDate));
      if (showAgeOfChange)
        appendElapsedTime( sb, this.changeDate );
    }
    return sb.toString();
  }

  final static List<TimeUnit> timeUnits = Arrays.asList(
      TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS);

  public static void appendElapsedTime( StringBuilder sb, Timestamp since ) {

    long seconds = java.time.Duration.between(
        since.toInstant(), java.time.Instant.now()).getSeconds();
    //java.time.ZonedDateTime.now().toInstant()).getSeconds();
    if ( seconds == 0 ) return;

    sb.append(" (");

    if (seconds < 0) { seconds = Math.abs(seconds); sb.append('-'); }

    for ( int i = 0; i < timeUnits.size() ; i++ ) {

      TimeUnit t = timeUnits.get(i);
      long unitCount = t.convert(seconds, TimeUnit.SECONDS);
      if ( unitCount == 0 ) continue;// the first time we pass this point is the last loop

      // append most significant time units
      sb.append( unitCount ).append(' ').append( t.toString().toLowerCase() );

      // if the most significant was the last candidate, we're done
      if ( i+1 == timeUnits.size() ) break;

      // otherwise, consider only the immediate next less significant unit
      seconds -= TimeUnit.SECONDS.convert( unitCount, t);
      t = timeUnits.get(i+1);
      unitCount = t.convert(seconds, TimeUnit.SECONDS);
      if ( unitCount == 0 ) break;
      sb.append(", ").append( unitCount ).append(' ').append( t.toString().toLowerCase() );
      break;
    }
    sb.append(')');
  }

  public enum Type { BIB, HOLDING, ITEM, ITEM_BATCH, CIRC, AGE, RECORD, ORDER, INSTANCE, LOAN }

  @Override
  public boolean equals( Object o ) {
    if (this == o) return true;
    if (o == null) return false;
    if (this.getClass() != o.getClass()) return false;
    Change other = (Change) o;
    return Objects.equals( this.type,       other.type)
        && Objects.equals( this.changeDate, other.changeDate)
        && Objects.equals( this.detail,     other.detail)
        && Objects.equals( this.location,   other.location);
  }

  @Override
  public int compareTo(Change o) {
    if ( ! this.type.equals( o.type ) ) {
      System.out.println(this.type+":"+o.type);
      return this.type.compareTo( o.type );
    }
    System.out.println("Same type");
    if ( ! this.changeDate.equals( o.changeDate ) )
      return this.changeDate.compareTo( o.changeDate );
    System.out.println("Same timestamp");
    if ( this.detail == null )
      return ( o.detail == null ) ? 0 : -1;
    if ( ! this.detail.equals( o.detail ) )
      return this.detail.compareTo( o.detail );
    System.out.println("Same detail");
    if ( this.location == null )
      return ( o.location == null ) ? 0 : -1;
    if ( ! this.location.equals( o.location ) )
      return this.location.compareTo( o.location );
    System.out.println("Same location");

    return 0;
  }

  @Override
  public int hashCode() {
    return this.toString(false).hashCode();
  }

}
