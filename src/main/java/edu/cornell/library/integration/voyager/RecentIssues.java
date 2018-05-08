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

public class RecentIssues {

  private final static String recentByHoldingQuery =
      "SELECT si.enumchron" +
      "  FROM line_item_copy_status lics, subscription s, component c, issues_received ir, serial_issues si"+
      " WHERE lics.mfhd_id = ?"+
      "   AND lics.line_item_id = s.line_item_id"+
      "   AND s.subscription_id = c.subscription_id"+
      "   AND c.component_id = si.component_id"+
      "   AND c.component_id = ir.component_id"+
      "   AND ir.issue_id = si.issue_id"+
      "   AND ir.opac_suppressed = 1"+//note: 0 = true, 1 = false
      " ORDER BY si.issue_id DESC";

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
      "SELECT distinct li.bib_id" +
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

  public static Set<Integer> detectNewReceiptBibs( Connection voyager, Timestamp since ) throws SQLException {

    Set<Integer> issues = new HashSet<>();
    try (  PreparedStatement pstmt = voyager.prepareStatement(newReceiptsBibsQuery)   ) {
      pstmt.setTimestamp(1,since);
      try (  ResultSet rs = pstmt.executeQuery()  ) {
        while (rs.next()) {
          issues.add( rs.getInt("bib_id") );
        }
      }
    }
    return issues;
  }

  public static List<String> getByHoldingId( Connection voyager, Integer holdingId ) throws SQLException {

    List<String> issues = new ArrayList<>();
    try (  PreparedStatement pstmt = voyager.prepareStatement(recentByHoldingQuery)   ) {
      pstmt.setInt(1, holdingId);
      try (  ResultSet rs = pstmt.executeQuery()  ) {
        while(rs.next())
          issues.add(rs.getString("enumchron"));
      }
    }
    return issues;
  }

  public static Map<Integer,List<String>> getByBibId( Connection voyager, Integer bibId ) throws SQLException {

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


}
