package edu.cornell.library.integration.folio;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;
import edu.cornell.library.integration.folio.Holding.Link;
import edu.cornell.library.integration.folio.Items.ItemList;

public class Holdings {

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

  public static HoldingSet retrieveHoldingsByInstanceId(
      OkapiClient okapi, Locations locations, ReferenceData holdingsNoteTypes, String instanceId )
      throws IOException {
    List<Map<String, Object>> rawHoldings =
        okapi.queryAsList("/holdings-storage/holdings", "instanceId=="+instanceId, null);
    HoldingSet holdings = new HoldingSet();
    for ( Map<String,Object> rawHolding : rawHoldings)
      holdings.put((String)rawHolding.get("id"),new Holding(rawHolding,locations,holdingsNoteTypes));
    return holdings;
  }

  public static HoldingSet retrieveHoldingsByInstanceHrid(
      Connection inventory, Locations locations, ReferenceData holdingsNoteTypes, String instanceHrid )
      throws SQLException,IOException {
    if ( holdingsByInstance == null )
      holdingsByInstance = inventory.prepareStatement("SELECT * FROM holdingFolio WHERE instanceHrid = ?");
    holdingsByInstance.setString(1, instanceHrid);
    HoldingSet holdings = new HoldingSet();
    try (ResultSet rs = holdingsByInstance.executeQuery() ) {
      while (rs.next()) {
        holdings.put(rs.getString("id"),
            new Holding(mapper.readValue(rs.getString("content"),Map.class),locations,holdingsNoteTypes));
      }
    }
    return holdings;
  }

  public static HoldingSet retrieveHoldingsByInstanceHrid(
      OkapiClient okapi, Locations locations, ReferenceData holdingsNoteTypes, String instanceHrid )
      throws IOException {

    List<Map<String,Object>> instances = okapi.queryAsList("/instance-storage/instances", "hrid=="+instanceHrid, null);
    if (instances.size() != 1) {
      System.out.printf("%d instances found for hrid %s\n",instances.size(),instanceHrid);
      System.exit(0);
    }
    Map<String,Object> instance = instances.get(0);
    String instanceId = (String)instance.get("id");
    return retrieveHoldingsByInstanceId(okapi,locations,holdingsNoteTypes,instanceId);
  }


  public static HoldingSet extractHoldingsFromJson( String json ) throws IOException {
    return mapper.readValue(json, HoldingSet.class);
  }
  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  public static class HoldingSet {
    private Map<String,Holding> holdings;

    @JsonCreator
    public HoldingSet( Map<String,Holding> holdings ) {
      this.holdings = holdings;
    }

    public HoldingSet() {
      this.holdings = new LinkedHashMap<>();
    }
    public HoldingSet(String uuid, Holding holding) {
      this.holdings = new LinkedHashMap<>();
      this.holdings.put(uuid, holding);
    }
    public void put(String uuid, Holding holding) {
      this.holdings.put(uuid, holding);
    }
    public int size() {
      return this.holdings.size();
    }
    public Holding get( String uuid ) {
      return this.holdings.get(uuid);
    }
    public Collection<Holding> values() {
      return this.holdings.values();
    }

    public Set<String> getUuids( ) {
      return this.holdings.keySet();
    }

    public LinkedHashSet<String> getNotes() {
      LinkedHashSet<String> notes = new LinkedHashSet<>();
      for (Holding h : this.holdings.values())
        if ( h.notes != null && ! h.notes.isEmpty() )
          notes.addAll(h.notes);
      return notes;
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


    public boolean summarizeItemAvailability( ItemList items ) {
      boolean discharged = false;
      for ( Entry<String, Holding> e : this.holdings.entrySet() )
        if ( e.getValue().online == null || ! e.getValue().online )
          if ( e.getValue().summarizeItemAvailability(items.getItems().get(e.getKey())) )
            discharged = true;
      return discharged;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this.holdings);
    }
/*TODO
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
*/
    public boolean hasRecent() {
      for ( Holding h : this.holdings.values() )
        if ( h.recentIssues != null )
          return true;
      return false;
    }
/*
    public Set<String> getBoundWithBarcodes() {
      Set<String> barcodes = new HashSet<>();
      for ( Holding h : this.holdings.values() ) {
        if ( h.boundWiths == null )
          continue;
        for ( BoundWith bw : h.boundWiths.values() )
          barcodes.add(bw.barcode);
      }
      return barcodes;
    }*/
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
      if (l.desc != null && (l.url.contains("hathitrust") || l.url.contains("handle.net/2027/"))) {

        if (hathiHolding == null) {
          hathiHolding = new Holding(
              null, null, null, null, null, null, null,
              null, null, null, null, null, null, null,
              true/*online*/,null,true/*active*/);
          hathiHolding.links = new ArrayList<>();
          holdings.put("0", hathiHolding);
        }
        hathiHolding.links.add(l);
      } else if ( onlineHolding != null ) {
        if ( onlineHolding.links == null )
          onlineHolding.links = new ArrayList<>();
        onlineHolding.links.add(l);
      }
    }
   }

  private static PreparedStatement holdingsByInstance = null;
}

