package edu.cornell.library.integration.voyager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.changes.ChangeDetector;
import edu.cornell.library.integration.voyager.Items.Item;

public class ReserveModule implements ChangeDetector {

  private static final String recentItemReserveStatusChangesQuery =
      "SELECT item_id, effect_date, expire_date"+
      "  FROM reserve_item_history"+
      " WHERE effect_date > ? OR expire_date > ?";
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

    try (PreparedStatement pstmt = voyager.prepareStatement(recentItemReserveStatusChangesQuery);
        PreparedStatement bibStmt = voyager.prepareStatement(bibIdFromItemIdQuery)) {

      pstmt.setTimestamp(1,since);
      pstmt.setTimestamp(2,since);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          Item item = Items.retrieveItemByItemId( voyager, rs.getInt("item_id"));
          Timestamp modDate = rs.getTimestamp(3);
          if (modDate == null) modDate = rs.getTimestamp(2);
          bibStmt.setInt(1, item.itemId);
          try( ResultSet bibRs = bibStmt.executeQuery() ) {
            while (bibRs.next()) {
              Change c = new Change(Change.Type.RESERVE,String.valueOf(item.itemId),
                  ((item.enumeration!= null)?item.itemId+" ("+item.enumeration+")":String.valueOf(item.itemId)),
                  modDate, item.location.name);
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

}
