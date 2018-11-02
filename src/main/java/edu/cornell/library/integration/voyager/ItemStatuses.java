package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ItemStatuses {

  private static Set<Integer> unavailableStatuses = 
      new HashSet<>( Arrays.asList(2,3,4,5,6,7,8,9,10,12,13,14,18,21,22,23,24,25));

  public static boolean getIsUnavailable( int id ) {
    return unavailableStatuses.contains(id);
  }

  public static StatusCode getStatusByCode( Connection voyager, int id ) throws SQLException {
    if (_statuses.isEmpty())
      populateStatusMap(voyager);
    if (_statuses.containsKey(id))
      return _statuses.get(id);
    return null;
  }

  private static void populateStatusMap( Connection voyager ) throws SQLException {
    String q = "SELECT * FROM item_status_type";
    try ( Statement stmt = voyager.createStatement(); ResultSet rs = stmt.executeQuery(q)) {
      while (rs.next())
        _statuses.put(rs.getInt("item_status_type"),
            new StatusCode(rs.getInt("item_status_type"),rs.getString("item_status_desc")));
    }
  }
  private static Map<Integer,StatusCode> _statuses = new HashMap<>();

  public static class StatusCode implements Comparable<StatusCode> {
    final int id;
    final String name;
    final Integer priority;

    private StatusCode (int id, String name) {
      this.id = id;
      this.name = name;
      if (_statusPriorities.contains(name))
        this.priority = _statusPriorities.indexOf(name);
      else {
        this.priority = null;
        System.out.println("Warning: status code "+id+" ("+name+") missing from status priority list - not ranked." );
      }
    }

    @Override
    public int compareTo(final StatusCode o) {
      if (this.priority == null)
        return (o.priority == null)?0:1;
      if (o.priority == null)
        return -1;
      return this.priority.compareTo(o.priority);
    }

    @Override
    public int hashCode() {
      return Integer.hashCode( this.id );
    }

    @Override
    public boolean equals( final Object o) {
      if (this == o) return true;
      if (o == null) return false;
      if ( ! this.getClass().equals(o.getClass()) ) return false;
      return this.id == ((StatusCode)o).id;
      }
  }

  /* While the association between status codes used directly by item records and their descriptive labels
   * is kept in the item_status_type table, a separate association between the descriptive labels and their
   * relative priority "rank" is kept in the Circulation User's Guide for the Voyager system (and presumably
   * in the code where they are applied for the public UI and other tools). As of Voyager 10.0.0, and the
   * version of the manual dated May 2017, this table runs from page 5-20 through 5-22. I've adjusted some
   * of the display names appearing in that documentation table to better match the ones from the Voyager
   * database table.
   * 
   * Nov 2018: Based on some erroneously visible withdrawn items, we've modified the priority order to put
   * Withdrawn just above Discharged in the order. This ensures that when an item's statuses are Withdrawn
   * and Not Charged, the Withdrawn status will be the higher ranking status and the item will not appear
   * available. The original ranking had Withdrawn at the very end. (DISCOVERYACCESS-4695)
   */
  private static List<String> _statusPriorities = Arrays.asList(
      "Scheduled",
      "In Process",
      "Lost--System Applied",
      "Lost--Library Applied",
      "Missing",
      "At Bindery",
      "Charged",
      "Renewed",
      "Overdue",
      "On Hold",
      "In Transit",
      "In Transit Discharged",
      "In Transit On Hold",
      "Recall Request",
      "Hold Request",
      "Short Loan Request",
      "Remote Storage Request",
      "Call Slip Request",
      "Withdrawn",
      "Discharged",
      "Not Charged",
      "Cataloging Review",
      "Circulation Review",
      "Claims Returned",
      "Damaged");
}
