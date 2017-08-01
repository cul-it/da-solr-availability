package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ItemStatuses {

  private static Set<Integer> unavailableStatuses = 
      new HashSet<>( Arrays.asList(2,3,4,5,6,7,8,9,10,12,13,14,18,21,22,23,24,25));

  public static boolean getIsUnvailable( int id ) {
    return unavailableStatuses.contains(id);
  }

  public static String getStatusNameById( Connection voyager, int id ) throws SQLException {
    if (_statusMap.isEmpty())
      populateStatusMap(voyager);
    if (_statusMap.containsKey(id))
      return _statusMap.get(id);
    return null;
  }

  private static void populateStatusMap( Connection voyager ) throws SQLException {
    String q = "SELECT * FROM item_status_type";
    try ( Statement stmt = voyager.createStatement(); ResultSet rs = stmt.executeQuery(q)) {
      while (rs.next())
        _statusMap.put(rs.getInt("item_status_type"),rs.getString("item_status_desc"));
    }
  }
  private static Map<Integer,String> _statusMap = new HashMap<>();
  
}
