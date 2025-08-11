package edu.cornell.library.integration.exports;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class ExportUtils {

  static void cleanUnwantedDataFields(
      MarcRecord bibRec, List<String> ids,List<FieldRange> ranges, boolean cleanNonNumeric) {
    Set<DataField> unwanted = new HashSet<>();

    // evaluate fields against elimination conditions
    for (DataField f : bibRec.dataFields) {

      if (f.tag.length() != 3 ||
          ! Character.isDigit(f.tag.charAt(0)) ||
          ! Character.isDigit(f.tag.charAt(1)) ||
          ! Character.isDigit(f.tag.charAt(2))) {
        if (cleanNonNumeric) unwanted.add(f);
      }
      if (ids != null && ids.contains(f.tag)) {
        unwanted.add(f);
        continue;
      }
      if (ranges != null)
        for (FieldRange r : ranges) 
          if (f.tag.compareTo(r.from) >= 0 && f.tag.compareTo(r.to) <= 0) {
            unwanted.add(f);
            continue;
          }

    }

    // remove identified fields
    for (DataField f : unwanted)
      bibRec.dataFields.remove(f);
  }
  public static class FieldRange{
    String from; String to;
    FieldRange (String from, String to) {this.from = from;this.to = to;}
  }


  static Set<String> getBibsToExport(Connection inventory) throws SQLException {
    Set<String> bibs = new LinkedHashSet<>();
    try ( Statement stmt = inventory.createStatement() ){
      stmt.setFetchSize(1_000_000);
      try ( ResultSet rs = stmt.executeQuery("SELECT hrid FROM instanceFolio ORDER BY RAND()")) {
        while ( rs.next() ) bibs.add(rs.getString(1));
      }
    }
    return bibs;
  }

  static Map<String, Object> getInstance(Connection inventory, String bibId)
      throws SQLException, IOException {
    try ( PreparedStatement instanceByHrid = inventory.prepareStatement
            ("SELECT * FROM instanceFolio WHERE hrid = ?") ) {
      instanceByHrid.setString(1, bibId);
      try ( ResultSet rs1 = instanceByHrid.executeQuery() ) {
        while (rs1.next()) return mapper.readValue( rs1.getString("content"), Map.class);
      }
    }
    return null;
  }
  private static ObjectMapper mapper = new ObjectMapper();

}
