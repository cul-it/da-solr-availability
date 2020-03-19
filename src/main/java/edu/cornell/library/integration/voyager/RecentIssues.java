package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.changes.ChangeDetector;

public class RecentIssues implements ChangeDetector {


  private final static String recentByBibQuery =
      "SELECT lics.mfhd_id, si.enumchron" +
      "  FROM line_item li, line_item_copy_status lics, subscription s,"+
      "       component c, issues_received ir, serial_issues si"+
      " WHERE li.bib_id = ?"+
      "   AND li.line_item_id = s.line_item_id"+
      "   AND li.line_item_id = lics.line_item_id"+
      "   AND s.subscription_id = c.subscription_id"+
      "   AND c.component_id = si.component_id"+
      "   AND c.component_id = ir.component_id"+
      "   AND ir.issue_id = si.issue_id"+
      "   AND ir.opac_suppressed = 1"+//note: 0 = true, 1 = false
      " ORDER BY si.issue_id DESC";

  private final static String newReceiptsBibsQuery =
      "SELECT li.bib_id, si.enumchron, si.receipt_date" +
      "  FROM line_item li, line_item_copy_status lics, subscription s,"+
      "       component c, issues_received ir, serial_issues si"+
      " WHERE li.line_item_id = s.line_item_id"+
      "   AND li.line_item_id = lics.line_item_id"+
      "   AND s.subscription_id = c.subscription_id"+
      "   AND c.component_id = si.component_id"+
      "   AND c.component_id = ir.component_id"+
      "   AND ir.issue_id = si.issue_id"+
      "   AND ir.opac_suppressed = 1"+//note: 0 = true, 1 = false
      "   AND si.receipt_date > ?";

  private final static String allBibsQuery =
      "SELECT distinct li.bib_id" +
      "  FROM line_item li, subscription s,"+
      "       component c, issues_received ir, serial_issues si"+
      " WHERE li.line_item_id = s.line_item_id"+
      "   AND s.subscription_id = c.subscription_id"+
      "   AND c.component_id = si.component_id"+
      "   AND c.component_id = ir.component_id"+
      "   AND ir.issue_id = si.issue_id"+
      "   AND ir.opac_suppressed = 1";//note: 0 = true, 1 = false

  static Set<Integer> getAllAffectedBibs( Connection voyager ) throws SQLException {

    Set<Integer> bibIds = new HashSet<>();
    try (  PreparedStatement pstmt = voyager.prepareStatement(allBibsQuery);
           ResultSet rs = pstmt.executeQuery()  ) {

      while (rs.next()) bibIds.add( rs.getInt("bib_id") );
    }
    return bibIds;
  }

  @Override
  public Map<Integer,Set<Change>> detectChanges( // specifically new receipts
      Connection voyager, Timestamp since ) throws SQLException {

    Map<Integer,Set<Change>> changes = new HashMap<>();

    try (  PreparedStatement pstmt = voyager.prepareStatement(newReceiptsBibsQuery)   ) {
      pstmt.setTimestamp(1,since);
      try (  ResultSet rs = pstmt.executeQuery()  ) {
        while (rs.next()) {
          Integer bibId = rs.getInt("bib_id");
          Change c = new Change(Change.Type.SERIALISSUES,null,rs.getString("enumchron"),
              rs.getTimestamp("receipt_date"),null);
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
    return changes;
  }

  public static Map<Integer,Set<Change>> detectAllChangedBibs(
      Connection voyager, Map<Integer,String> prevValues, Map<Integer,Set<Change>> changes )
          throws SQLException, JsonProcessingException {
    Set<Integer> currentBibs = getAllAffectedBibs( voyager );
    for(Integer bibId : prevValues.keySet()) {
      if (currentBibs.contains(bibId)) {
        String updatedJson = mapper.writeValueAsString(getByBibId(voyager,bibId));
        if ( ! updatedJson.equals(prevValues.get(bibId))) {
          Set<Change> t = new HashSet<>();
          t.add(new Change(Change.Type.SERIALISSUES,null,"Recent Issues Updated",null,null));
          changes.put(bibId,t);
        }
        currentBibs.remove(bibId);
      } else {
        Set<Change> t = new HashSet<>();
        t.add(new Change(Change.Type.SERIALISSUES,null,"Recent Issues Suppressed",null,null));
        changes.put(bibId,t);
      }
    }
    for (Integer bibId : currentBibs) {
      Set<Change> t = new HashSet<>();
      t.add(new Change(Change.Type.SERIALISSUES,null,"Recent Issues Added",null,null));
      changes.put(bibId,t);
    }
    return changes;
  }

  static Map<Integer,List<String>> getByBibId( Connection voyager, Integer bibId ) throws SQLException {

    Map<Integer,List<String>> issues = new HashMap<>();
    try (  PreparedStatement pstmt = voyager.prepareStatement(recentByBibQuery)   ) {
      pstmt.setInt(1, bibId);
      try (  ResultSet rs = pstmt.executeQuery()  ) {
        while (rs.next()) {
          Integer mfhdId = rs.getInt("mfhd_id");
          if ( ! issues.containsKey(mfhdId))
            issues.put(mfhdId, new ArrayList<String>());
          issues.get(mfhdId).add(rs.getString("enumchron"));
        }
      }
    }
    return issues;
  }

  private static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

}
