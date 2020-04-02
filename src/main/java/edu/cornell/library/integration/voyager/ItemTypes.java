package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemTypes {

  private static List<String> shortLoanItemTypes =
      Arrays.asList("1hrloan","1hrres","2hrloan","2hrres","3hrloan","3hrres","4hrloan","4hrres","8hrres","permres","nocirc");

  public ItemTypes( final Connection voyager) throws SQLException {
    if (this._types.isEmpty())
      populateTypes(voyager);
  }

  public ItemType getById( int id ) {
    if (this._types.containsKey(id))
      return this._types.get(id);
    return null;
  }

 public static class ItemType{
    public final int id;
    public final String name;
    @JsonCreator ItemType( @JsonProperty("id") int id, @JsonProperty("name") String name) {
      this.id = id;
      this.name = name;
    }
  }

 public static boolean isShortLoanType( ItemType type ) {
   if (type == null || type.name == null || type.name.isEmpty())
     return false;
   return shortLoanItemTypes.contains(type.name);
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
        this._types.put(id, new ItemType(id,name));
      }
    }
  }
}
