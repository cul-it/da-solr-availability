package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemTypes {

  public ItemTypes( final Connection voyager) throws SQLException {
    if (_types.isEmpty())
      populateTypes(voyager);
  }

  public ItemType getById( int id ) {
    if (_types.containsKey(id))
      return _types.get(id);
    return null;
  }

 public static class ItemType{
    public final int id;
    public final String name;
    @JsonCreator
    private ItemType( @JsonProperty("id") int id, @JsonProperty("name") String name) {
      this.id = id;
      this.name = name;
    }
  }

  // PRIVATE RESOURCES

  private Map<Integer,ItemType> _types = new HashMap<>();

  private static final String getItemTypesQuery =
      "SELECT ITEM_TYPE_ID, ITEM_TYPE_NAME FROM ITEM_TYPE";

  private void populateTypes( final Connection voyager ) throws SQLException {
    try (Statement stmt = voyager.createStatement(); ResultSet rs = stmt.executeQuery(getItemTypesQuery)) {
      while (rs.next()) {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        _types.put(id, new ItemType(id,name));
      }
    }
  }
}
