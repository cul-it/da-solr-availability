package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CircPolicyGroups {

  public CircPolicyGroups( final Connection voyager) throws SQLException {
    if (_groups.isEmpty())
      populateGroups(voyager);
  }

  public Map<Integer,String> getByLocId( int locId ) {
    if (_groups.containsKey(locId))
      return _groups.get(locId).group;
    return null;
  }

 public static class CircPolicyGroup{
    public final Map<Integer,String> group = new HashMap<>();
    @JsonCreator
    private CircPolicyGroup( @JsonProperty("id") int id, @JsonProperty("name") String name) {
      group.put(id, name);
    }
  }

  // PRIVATE RESOURCES

  private Map<Integer,CircPolicyGroup> _groups = new HashMap<>();

  private static final String getCircPolicyGroupsQuery =
      "SELECT circ_policy_group.circ_group_id, circ_group_name, location_id " +
      "  FROM circ_policy_locs, circ_policy_group " +
      " WHERE circ_policy_group.circ_group_id = circ_policy_locs.circ_group_id ";

  private void populateGroups( final Connection voyager ) throws SQLException {
    try (Statement stmt = voyager.createStatement(); ResultSet rs = stmt.executeQuery(getCircPolicyGroupsQuery)) {
      while (rs.next()) {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        _groups.put(rs.getInt(3), new CircPolicyGroup(id,name));
      }
    }
  }
}
