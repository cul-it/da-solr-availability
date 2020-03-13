package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.voyager.Holding.Link;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class Holdings {

  private static final String holdingsByBibIdQuery =
      "SELECT *"+
      "  FROM bib_mfhd, mfhd_master"+
      " WHERE bib_mfhd.mfhd_id = mfhd_master.mfhd_id"+
      "   AND bib_mfhd.bib_id = ?"+
      " ORDER BY bib_mfhd.mfhd_id";
  private static final String holdingByHoldingIdQuery =
      "SELECT *"+
      "  FROM bib_mfhd, mfhd_master"+
      " WHERE bib_mfhd.mfhd_id = mfhd_master.mfhd_id"+
      "   AND bib_mfhd.mfhd_id = ?";
  private static final String recentHoldingsChangesQuery =
      "select bib_id, update_date, create_date, mfhd_master.mfhd_id"+
      "  from mfhd_master, bib_mfhd "+
      " where suppress_in_opac = 'N'"+
      "   and (update_date > ? or create_date > ? )"+
      "   and bib_mfhd.mfhd_id = mfhd_master.mfhd_id";

  public static Map<Integer,Set<Change>> detectChangedHoldings(
      Connection voyager, Timestamp since, Map<Integer,Set<Change>> changedBibs ) throws SQLException {

    try ( PreparedStatement pstmt = voyager.prepareStatement( recentHoldingsChangesQuery )){
      pstmt.setTimestamp(1, since);
      pstmt.setTimestamp(2, since);
      try( ResultSet rs = pstmt.executeQuery() ) {
        while (rs.next()) {
          Change c ;
          if (rs.getTimestamp(2) == null)
            c = new Change(Change.Type.HOLDING,rs.getInt(4),"New ("+rs.getInt(4)+")",rs.getTimestamp(3),null);
          else
            c = new Change(Change.Type.HOLDING,rs.getInt(4),"Update ("+rs.getInt(4)+")",rs.getTimestamp(2),null);
          if (changedBibs.containsKey(rs.getInt(1)))
            changedBibs.get(rs.getInt(1)).add(c);
          else {
            Set<Change> t = new HashSet<>();
            t.add(c);
            changedBibs.put(rs.getInt(1),t);
          }
        }
      }
    }
    return changedBibs;
  }

  public static HoldingSet retrieveHoldingsByBibId( Connection voyager, int bib_id )
      throws SQLException, IOException, XMLStreamException {
    HoldingSet holdings = new HoldingSet();
    try (PreparedStatement pstmt = voyager.prepareStatement(holdingsByBibIdQuery)) {
      pstmt.setInt(1, bib_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          holdings.put(rs.getInt("mfhd_id"),new Holding(voyager,rs));
      }
    }
    return holdings;
  }

  public static HoldingSet retrieveHoldingsByHoldingId( Connection voyager, int mfhd_id )
      throws SQLException, IOException, XMLStreamException {
    try (PreparedStatement pstmt = voyager.prepareStatement(holdingByHoldingIdQuery)) {
      pstmt.setInt(1, mfhd_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          return new HoldingSet( mfhd_id, new Holding(voyager,rs));
      }
    }
    return null;
  }

  public static HoldingSet extractHoldingsFromJson( String json ) throws IOException {
    return mapper.readValue(json, HoldingSet.class);
  }
  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  public static class HoldingSet {
    private Map<Integer,Holding> holdings;

    @JsonCreator
    public HoldingSet( Map<Integer,Holding> holdings ) {
      this.holdings = holdings;
    }

    public HoldingSet() {
      this.holdings = new LinkedHashMap<>();
    }
    public HoldingSet(Integer mfhdId, Holding holding) {
      this.holdings = new LinkedHashMap<>();
      this.holdings.put(mfhdId, holding);
    }
    public void put(Integer mfhdId, Holding holding) {
      this.holdings.put(mfhdId, holding);
    }
    public int size() {
      return this.holdings.size();
    }
    public Holding get( Integer mfhdId ) {
      return this.holdings.get(mfhdId);
    }
    public Collection<Holding> values() {
      return this.holdings.values();
    }

    public Set<Integer> getMfhdIds( ) {
      return this.holdings.keySet();
    }

    public boolean noItemsAvailability() {
        boolean noItems = false;
        for ( Holding h : this.holdings.values() )
          if ( h.noItemsAvailability() )
            noItems = true;
        return noItems;
    }

    public Set<String> getLocationFacetValues() {

      Set<String> facetValues = new LinkedHashSet<>();
      for (Holding h : this.holdings.values()) {
        if (h.active == false)
          continue;
        if (h.online != null && h.online)
          continue;
        Set<String> holdingFacetValues = h.getLocationFacetValues();
        if (holdingFacetValues != null)
          facetValues.addAll(holdingFacetValues);
      }
      return facetValues;
    }


    public boolean applyOpenOrderInformation( Connection voyager, Integer bibId ) throws SQLException {
      OpenOrder order = new OpenOrder(voyager, bibId);
      if (order.notes.isEmpty()) return false;

      boolean onOrder = false;

      for ( Entry<Integer,String> e : order.notes.entrySet() ) {
        int mfhdId = e.getKey();
        String note = e.getValue();

        if (! this.holdings.containsKey(mfhdId)) {
          System.out.println( "Order note on bib "+bibId+" associated with holding "+mfhdId+
              ", which is not currently an active holding for this bib." );
          continue;
        }

        Holding h = this.holdings.get(mfhdId);
        // Block order note if:
        // * There are any items on the mfhd
        // * The holding call number is "Available for the Library to Purchase"
        // * The holding call number is "In Process"
        if (h.itemSummary == null
            && ! (h.call != null && (h.call.equalsIgnoreCase("Available for the Library to Purchase")
                                    || h.call.equalsIgnoreCase("In Process")))) {
          h.orderNote = note;
          onOrder = true;
        }

      }

      if ( ! onOrder ) return onOrder;

      // Add secondary order notes for multi-copy orders
      for ( Holding h : this.holdings.values() )
        if ( h.orderNote != null
            && ( h.orderNote.contains("copies ordered") || h.orderNote.startsWith("On order as of")) ) {
          for ( Holding h2 : this.holdings.values() )
            if ( h2.orderNote == null && h2.itemSummary == null
                && Objects.equals( h.location, h2.location )
                && ! (h2.call != null && (h2.call.equalsIgnoreCase("Available for the Library to Purchase")
                    || h2.call.equalsIgnoreCase("In Process")))) {
              h2.orderNote = "On order";
            }
        }

      return onOrder;
    }

    public boolean summarizeItemAvailability( ItemList items ) {
      boolean discharged = false;
      for ( Entry<Integer, Holding> e : this.holdings.entrySet() )
        if ( e.getValue().summarizeItemAvailability(items.getItems().get(e.getKey())) )
          discharged = true;
      return discharged;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this.holdings);
    }

    public void getRecentIssues(Connection voyager, Connection inventory, Integer bibId) throws SQLException {
      Map<Integer,List<String>> issues = RecentIssues.getByBibId(voyager, bibId);
      for (Entry<Integer,List<String>> e : issues.entrySet())
        if (this.holdings.containsKey(e.getKey()))
          this.holdings.get(e.getKey()).recentIssues = e.getValue();

      if ( inventory == null ) return;

      if ( issues.isEmpty() ) {
        try (PreparedStatement delStmt = inventory.prepareStatement("DELETE FROM recentIssues WHERE bib_id = ?")) {
          delStmt.setInt(1, bibId);
          delStmt.executeUpdate();
        }
        return;
      }

      String json;
      try { json = mapper.writeValueAsString(issues);  } catch (JsonProcessingException e) { e.printStackTrace(); return; }
      String oldJson = null;
      try (PreparedStatement selStmt = inventory.prepareStatement("SELECT json FROM recentIssues WHERE bib_id = ?")) {
        selStmt.setInt(1, bibId);
        try (ResultSet rs = selStmt.executeQuery()) {
          while (rs.next()) oldJson = rs.getString(1);
        }
      }

      if ( json.equals(oldJson) ) return;

      try (PreparedStatement updStmt = inventory.prepareStatement("REPLACE INTO recentIssues (bib_id, json) VALUES (?,?)")) {
        updStmt.setInt(1, bibId);
        updStmt.setString(2, json);
        updStmt.executeUpdate();
      }
    }

    public boolean hasRecent() {
      for ( Holding h : this.holdings.values() )
        if ( h.recentIssues != null )
          return true;
      return false;
    }
  }

  public static void mergeAccessLinksIntoHoldings(HoldingSet holdings, Collection<Object> linkJsons)
      throws IOException {

    Holding onlineHolding = null;
    Holding hathiHolding = null;

    for ( Holding h : holdings.values() )
      if ( h.online != null && h.online )
        onlineHolding = h;
    for ( Object linkJson : linkJsons ) {
      Link l = Link.fromJson((String)linkJson);
      if (l.desc != null && (l.desc.startsWith("HathiTrust"))) {
        if (hathiHolding == null) {
          hathiHolding = new Holding(
              null, null, null, null, null, null, null,null,null,null,true/*online*/,null,null,null,null,true);
          hathiHolding.links = new ArrayList<>();
          holdings.put(0, hathiHolding);
        }
        hathiHolding.links.add(l);
      } else if ( onlineHolding != null ) {
        if ( onlineHolding.links == null )
          onlineHolding.links = new ArrayList<>();
        onlineHolding.links.add(l);
      }
    }

   }

}
