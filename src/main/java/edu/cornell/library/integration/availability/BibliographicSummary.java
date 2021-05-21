package edu.cornell.library.integration.availability;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.changes.Change;

public class BibliographicSummary {

  @JsonProperty("available") public Boolean available;
  @JsonProperty("online")    public Boolean online = false;
  @JsonProperty("availAt")   public Map<String,String> availAt = new LinkedHashMap<>();
  @JsonProperty("unavailAt") public Map<String,String> unavailAt = new LinkedHashMap<>();

  @JsonIgnore static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

  public static Map<Integer,Set<Change>> detectChangedBibs(
      Connection voyager, Timestamp since, Map<Integer,Set<Change>> changedBibs ) throws SQLException {

    try ( PreparedStatement pstmt = voyager.prepareStatement(
        "select bib_id, update_date, create_date"+
        "  from bib_master"+
        " where suppress_in_opac = 'N'"+
        "   and (update_date > ? or create_date > ? )")){
      pstmt.setTimestamp(1, since);
      pstmt.setTimestamp(2, since);
      try( ResultSet rs = pstmt.executeQuery() ) {
        while (rs.next()) {
          Change c ;
          if (rs.getTimestamp(2) == null)
            c = new Change(Change.Type.BIB,rs.getInt(1),"New",rs.getTimestamp(3),null);
          else
            c = new Change(Change.Type.BIB,rs.getInt(1),"Update",rs.getTimestamp(2),null);
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


  public static BibliographicSummary summarizeHoldingAvailability( edu.cornell.library.integration.folio.Holdings.HoldingSet holdings ){

    BibliographicSummary b = new BibliographicSummary();
    for ( edu.cornell.library.integration.folio.Holding h : holdings.values() ) {

      if ( h.active == false ) continue;
      if ( h.online != null && h.online )
      {   b.online = true; continue;   }

      if ( h.location == null ) continue;

      if ((h.avail != null && h.avail) ||
          (h.itemSummary != null && h.itemSummary.availItemCount != null))
        b.availAt.put(h.location.name, h.call);
      else {
        if (h.orderNote != null)
          b.unavailAt.put(h.location.name, h.orderNote);
        else
          b.unavailAt.put(h.location.name, h.call);
      }

      if ( h.itemSummary != null && h.itemSummary.tempLocs != null ) {
        List<Integer> unavailableItems = new ArrayList<>();
        if ( h.itemSummary.unavail != null )
          for ( edu.cornell.library.integration.folio.ItemReference ir : h.itemSummary.unavail )
            unavailableItems.add( ir.itemId );
        for ( edu.cornell.library.integration.folio.ItemReference ir : h.itemSummary.tempLocs )
          if ( unavailableItems.contains(ir.itemId) ) {
            if (! b.unavailAt.containsKey(ir.location.name))
              b.unavailAt.put(ir.location.name, null);
          } else {
            if (! b.availAt.containsKey(ir.location.name))
              b.availAt.put(ir.location.name, null);
          }
      }
    }
    if (b.availAt.isEmpty()) {
      if (b.unavailAt.isEmpty() && b.online)
        b.available = null;
      else
        b.available = false;
      b.availAt = null;
    } else {
      b.available = true;
      for (String availLoc : b.availAt.keySet())
        b.unavailAt.remove(availLoc);
    }
    if (b.unavailAt.isEmpty())
      b.unavailAt = null;

    if ( ! b.online ) b.online = null;

    return b;

  }



  public static BibliographicSummary summarizeHoldingAvailability( edu.cornell.library.integration.voyager.Holdings.HoldingSet holdings ){

    BibliographicSummary b = new BibliographicSummary();
    for ( edu.cornell.library.integration.voyager.Holding h : holdings.values() ) {

      if ( h.active == false ) continue;
      if ( h.online != null && h.online )
      {   b.online = true; continue;   }

      if ( h.location == null ) continue;

      if ((h.avail != null && h.avail) ||
          (h.itemSummary != null && h.itemSummary.availItemCount != null))
        b.availAt.put(h.location.name, h.call);
      else {
        if (h.orderNote != null)
          b.unavailAt.put(h.location.name, h.orderNote);
        else
          b.unavailAt.put(h.location.name, h.call);
      }

      if ( h.itemSummary != null && h.itemSummary.tempLocs != null ) {
        List<Integer> unavailableItems = new ArrayList<>();
        if ( h.itemSummary.unavail != null )
          for ( edu.cornell.library.integration.voyager.ItemReference ir : h.itemSummary.unavail )
            unavailableItems.add( ir.itemId );
        for ( edu.cornell.library.integration.voyager.ItemReference ir : h.itemSummary.tempLocs )
          if ( unavailableItems.contains(ir.itemId) ) {
            if (! b.unavailAt.containsKey(ir.location.name))
              b.unavailAt.put(ir.location.name, null);
          } else {
            if (! b.availAt.containsKey(ir.location.name))
              b.availAt.put(ir.location.name, null);
          }
      }
    }
    if (b.availAt.isEmpty()) {
      if (b.unavailAt.isEmpty() && b.online)
        b.available = null;
      else
        b.available = false;
      b.availAt = null;
    } else {
      b.available = true;
      for (String availLoc : b.availAt.keySet())
        b.unavailAt.remove(availLoc);
    }
    if (b.unavailAt.isEmpty())
      b.unavailAt = null;

    if ( ! b.online ) b.online = null;

    return b;

  }

  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  public BibliographicSummary() {}

  public BibliographicSummary(
      @JsonProperty("available") Boolean available,
      @JsonProperty("online")    Boolean online,
      @JsonProperty("availAt")   Map<String,String> availAt,
      @JsonProperty("unavailAt") Map<String,String> unavailAt
      ) {
    this.available = available;
    this.online = online;
    this.availAt = availAt;
    this.unavailAt = unavailAt;
  }
}
