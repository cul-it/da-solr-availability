package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.changes.ChangeDetector;
import edu.cornell.library.integration.voyager.Items.Item;

public class ItemStatuses implements ChangeDetector {

  private static Set<Integer> unavailableStatuses = 
      new HashSet<>( Arrays.asList(2,3,4,5,6,7,8,9,10,12,13,14,18,21,22,23,24,25));

  private static final String allCallSlipsQuery =
      "select bmast.bib_id, item_status.item_id, ist.item_status_desc"+
      "  from item_status, item_status_type ist, mfhd_item mi, mfhd_master mm, bib_mfhd bm, bib_master bmast"+
      " where item_status.item_status = ist.item_status_type"+
      "   and ist.item_status_desc = 'Call Slip Request'"+
      "   and item_status.item_id = mi.item_id"+
      "   and mi.mfhd_id = mm.mfhd_id"+
      "   and mm.suppress_in_opac = 'N'"+
      "   and mi.mfhd_id = bm.mfhd_id"+
      "   and bm.bib_id = bmast.bib_id"+
      "   and bmast.suppress_in_opac = 'N'";
  private static final String recentItemStatusChangesQuery =
      "SELECT * FROM item_status WHERE item_status_date > ?";
  private static final String bibIdFromItemIdQuery =
      "SELECT b.bib_id       "+
      "  FROM bib_master b,  "+
      "       bib_mfhd bm,   "+
      "       mfhd_master m, "+
      "       mfhd_item mi   "+
      " WHERE b.suppress_in_opac = 'N'  "+
      "   AND b.bib_id = bm.bib_id      "+
      "   AND bm.mfhd_id = m.mfhd_id    "+
      "   AND m.suppress_in_opac = 'N'  "+
      "   AND m.mfhd_id = mi.mfhd_id    "+
      "   AND mi.item_id = ?            ";

  @Override
  public Map<Integer, Set<Change>> detectChanges(Connection voyager, Timestamp since) throws SQLException {

    Map<Integer,Set<Change>> changes = new HashMap<>();

    try (PreparedStatement pstmt = voyager.prepareStatement(recentItemStatusChangesQuery);
        PreparedStatement bibStmt = voyager.prepareStatement(bibIdFromItemIdQuery)) {

      pstmt.setTimestamp(1, since);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          Item item = Items.retrieveItemByItemId( voyager, rs.getInt("item_id"));
          if ( item == null ) {
            System.out.printf("It looks like an item (%d) was deleted right after changing status.\n",rs.getInt("item_id"));
            continue;
          }
          bibStmt.setInt(1, item.itemId);
          try( ResultSet bibRs = bibStmt.executeQuery() ) {
            while (bibRs.next()) {
              Change c = new Change(Change.Type.CIRC,item.itemId,
                  String.join( " ",((item.enumeration!= null)?"("+item.enumeration+")":""),
                      item.status.code.values().iterator().next()),
                  new Timestamp((item.status.date != null)?item.status.date*1000:System.currentTimeMillis()),
                  item.location.name);
              Integer bibId = bibRs.getInt(1);
              if (changes.containsKey(bibId))
                changes.get(bibId).add(c);
              else {
                Set<Change> t = new HashSet<>();
                t.add(c);
                changes.put(bibId,t);
              }
            }
          }
        }
      }
    }
    return changes;
  }

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

  public static Map<Integer,String> collectAllCallSlipRequests( Connection voyager )
      throws SQLException{
    Map<Integer,Map<Integer,String>> t = new HashMap<>();
    try ( PreparedStatement pstmt = voyager.prepareStatement(allCallSlipsQuery);
        ResultSet rs = pstmt.executeQuery()) {
      while (rs.next()) {
        Integer bibId = rs.getInt(1);
        if (! t.containsKey(bibId) ) t.put(bibId, new TreeMap<>());
        t.get(bibId).put(rs.getInt(2),rs.getString(3));
      }
    }
    Map<Integer,String> callSlipJsons = new HashMap<>();
    for (Entry<Integer,Map<Integer,String>> e : t.entrySet())
      try {
        callSlipJsons.put(e.getKey(), mapper.writeValueAsString(e.getValue()));
      }
    catch (JsonProcessingException e1) { e1.printStackTrace(); /* Unreachable? */ }
    return callSlipJsons;
  
  }
  private static ObjectMapper mapper = new ObjectMapper();

  private static void populateStatusMap( Connection voyager ) throws SQLException {
    String q = "SELECT * FROM item_status_type";
    try ( Statement stmt = voyager.createStatement(); ResultSet rs = stmt.executeQuery(q)) {
      while (rs.next())
        _statuses.put(rs.getInt("item_status_type"),
            new StatusCode(rs.getInt("item_status_type"),rs.getString("item_status_desc")));
    }
  }
  private static Map<Integer,StatusCode> _statuses = new HashMap<>();

  static class StatusCode implements Comparable<StatusCode> {
    final int id;
    final String name;
    final Integer priority;

    StatusCode (int id, String name) {
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
  static List<String> _statusPriorities = Arrays.asList(
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
