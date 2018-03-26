package edu.cornell.library.integration.availability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.ItemReference;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;

public class BibliographicSummary {

  @JsonProperty("available") public Boolean available;
  @JsonProperty("online")    public Boolean online = false;
  @JsonProperty("multiLoc")  public Boolean multiLoc;
  @JsonProperty("availAt")   public Map<String,String> availAt = new LinkedHashMap<>();
  @JsonProperty("unavailAt") public Map<String,String> unavailAt = new LinkedHashMap<>();

  @JsonIgnore static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }


  public static BibliographicSummary summarizeHoldingAvailability( HoldingSet holdings ){

    BibliographicSummary b = new BibliographicSummary();
    for ( Holding h : holdings.values() ) {

      if ( h.online != null && h.online )
      {   b.online = true; continue;   }

      if ( h.location == null ) continue;

      if ((h.avail != null && h.avail) ||
          (h.itemSummary != null && h.itemSummary.availItemCount != null))
        b.availAt.put(h.location.name, h.call);
      else {
        if (h.openOrderNote != null)
          b.unavailAt.put(h.location.name, h.openOrderNote);
        else
          b.unavailAt.put(h.location.name, h.call);
      }

      if ( h.itemSummary != null && h.itemSummary.tempLocs != null ) {
        List<Integer> unavailableItems = new ArrayList<>();
        if ( h.itemSummary.unavail != null )
          for ( ItemReference ir : h.itemSummary.unavail )
            unavailableItems.add( ir.itemId );
        for ( ItemReference ir : h.itemSummary.tempLocs )
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
    } else {
      b.available = true;
      b.multiLoc = b.availAt.size() > 1;
    }
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
      @JsonProperty("multiLoc")  Boolean multiLoc,
      @JsonProperty("availAt")   Map<String,String> availAt,
      @JsonProperty("unavailAt") Map<String,String> unavailAt
      ) {
    this.available = available;
    this.online = online;
    this.multiLoc = multiLoc;
    this.availAt = availAt;
    this.unavailAt = unavailAt;
  }
}
