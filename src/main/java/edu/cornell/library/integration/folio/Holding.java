package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.Items.Item;
import edu.cornell.library.integration.folio.LoanTypes.ExpectedLoanType;
import edu.cornell.library.integration.folio.Locations.Location;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class Holding {

  @JsonProperty("hrid")        public final String hrid;
  @JsonProperty("copy")        public String copy;
  @JsonProperty("notes")       public List<String> notes;
  @JsonProperty("holdings")    public final List<String> holdings;
  @JsonProperty("supplements") public final List<String> supplements;
  @JsonProperty("indexes")     public final List<String> indexes;
  @JsonProperty("recents")     public List<String> recentIssues = null;

  @JsonProperty("location")    public Location location;
  @JsonProperty("call")        public String call;
  @JsonProperty("boundWith")   public Map<String,BoundWith> boundWiths;
  @JsonProperty("items")       public HoldingsItemSummary itemSummary = null;
  @JsonProperty("order")       public String orderNote = null;
  @JsonProperty("avail")       public Boolean avail = null;
  @JsonProperty("circ")        public Boolean circ = null;
  
  @JsonProperty("online")      public Boolean online = null;
  @JsonProperty("date")        public Integer date;
  @JsonProperty("links")       public List<Link> links = null;
  @JsonProperty("active")      public boolean active = true;


  @JsonIgnore public List<String> donors = null;
  @JsonIgnore public String callNumberSuffix = null;
  @JsonIgnore public boolean lcCallNum = false;
  @JsonIgnore static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }


  Holding(Map<String,Object> raw, Locations locations, ReferenceData holdingsNoteTypes) {

    Map<String,Object> metadata = (Map<String,Object>)raw.get("metadata");
    if ( metadata.containsKey("UpdatedDate") && metadata.get("UpdatedDate") != null )
      this.date = (int) Instant.parse((String)metadata.get("UpdatedDate")).getEpochSecond();
    else if ( metadata.containsKey("CreatedDate") && metadata.get("CreatedDate") != null )
      this.date = (int) Instant.parse((String)metadata.get("CreatedDate")).getEpochSecond();
    this.hrid = (String)raw.get("hrid");
    if ( raw.containsKey("discoverySuppress") )
      this.active = ! (boolean)raw.get("discoverySuppress");
    else
      this.active = true;

    if ( raw.containsKey("permanentLocationId") ) {
      Location l = locations.getByUuid((String)raw.get("permanentLocationId"));
      if (l.equals(locations.getByCode("serv,remo")))
        this.online = true;
      else
        this.location = l;
    }

    if ( raw.containsKey("callNumber") ) {
      String call = (String)raw.get("callNumber");
      if ( call != null ) {
        call =  call.replaceAll("[Nn]o [Cc]all [Nn]umber\\.*", "").trim();
        if (! call.isEmpty() ) this.call = call;
      }
    }
    if ( raw.containsKey("callNumberSuffix") )
      this.callNumberSuffix = (String) raw.get("callNumberSuffix");
    if ( raw.containsKey("copyNumber") )
      this.copy = (String) raw.get("copyNumber");

    if ( raw.containsKey("notes") ) {
      List<Map<String,Object>> notes = (List<Map<String,Object>>)raw.get("notes");
      for ( Map<String,Object> note : notes ) {
        String type = holdingsNoteTypes.getName((String) note.get("holdingsNoteTypeId"));
        String text = (String) note.get("note");
        boolean staffOnly = (boolean) note.get("staffOnly");

        if ( type.equals("Bound with item data") ) {
          BoundWith b = BoundWith.fromNote(note);
          if (b != null) boundWiths.put(b.masterItemId,b);
          continue;
        }

        if ( staffOnly ) continue;

        if ( type.equals("Source of acquisition") /*541*/) {
            if ( this.donors == null ) this.donors = new ArrayList<>();
            this.donors.add(text);

        } else if (
            type.equals("Restriction") /*506*/ ||
            type.equals("Provenance") /*561*/ ||
            type.equals("Copy note") /*562*/ ||
            type.equals("Reproduction") /*843*/ ||
            type.equals("Note") /*852$z*/
            ) {
          if ( this.notes == null ) this.notes = new ArrayList<>();
          this.notes.add(text);
        }
      }
    }

    if ( raw.containsKey("holdingsStatements") ) {
      List<Map<String,String>> a = (List<Map<String,String>>)raw.get("holdingsStatements");
      List<String> holdings = new ArrayList<>();
      for (Map<String,String> statement : a) {
        List<String> parts = new ArrayList<>();
        if ( statement.containsKey("statement") ) parts.add(statement.get("statement"));
        if ( statement.containsKey("note") )      parts.add(statement.get("note"));
        if ( parts.size() > 0 ) holdings.add(insertSpaceAfterCommas(String.join(" ", parts)));
      }
      this.holdings = holdings;
    } else
      this.holdings = null;

    if ( raw.containsKey("holdingsStatementsForSupplements") ) {
      List<Map<String,String>> a = (List<Map<String,String>>)raw.get("holdingsStatementsForSupplements");
      List<String> holdings = new ArrayList<>();
      for (Map<String,String> statement : a) {
        List<String> parts = new ArrayList<>();
        if ( statement.containsKey("statement") ) parts.add(statement.get("statement"));
        if ( statement.containsKey("note") )      parts.add(statement.get("note"));
        if ( parts.size() > 0 ) holdings.add(insertSpaceAfterCommas(String.join(" ", parts)));
      }
      this.supplements = holdings;
    } else
      this.supplements = null;

    if ( raw.containsKey("holdingsStatementsForIndexes") ) {
      List<Map<String,String>> a = (List<Map<String,String>>)raw.get("holdingsStatementsForIndexes");
      List<String> holdings = new ArrayList<>();
      for (Map<String,String> statement : a) {
        List<String> parts = new ArrayList<>();
        if ( statement.containsKey("statement") ) parts.add(statement.get("statement"));
        if ( statement.containsKey("note") )      parts.add(statement.get("note"));
        if ( parts.size() > 0 ) holdings.add(insertSpaceAfterCommas(String.join(" ", parts)));
      }
      this.indexes = holdings;
    } else
      this.indexes = null;

  }

  Holding(
      @JsonProperty("hrid")        String hrid,
      @JsonProperty("copy")        String copy,
      @JsonProperty("notes")       List<String> notes,
      @JsonProperty("holdings")    List<String> holdings,
      @JsonProperty("supplements") List<String> supplements,
      @JsonProperty("indexes")     List<String> indexes,
      @JsonProperty("recents")     List<String> recentIssues,

      @JsonProperty("location")    Location location,
      @JsonProperty("call")        String call,
      @JsonProperty("boundWith")   Map<String,BoundWith> boundWiths,
      @JsonProperty("items")       HoldingsItemSummary itemSummary,
      @JsonProperty("order")       String openOrderNote,
      @JsonProperty("avail")       Boolean avail,
      @JsonProperty("circ")        Boolean circ,

      @JsonProperty("online")      Boolean online,
      @JsonProperty("date")        Integer date,
      @JsonProperty("active")      boolean active) {
    this.hrid = hrid;
    this.copy = copy;
    this.notes = (notes == null || notes.isEmpty()) ? null : notes;
    this.holdings = (holdings == null || holdings.isEmpty()) ? null : holdings;
    this.supplements = (supplements == null || supplements.isEmpty()) ? null : supplements;
    this.indexes = (indexes == null || indexes.isEmpty()) ? null : indexes;
    this.recentIssues = recentIssues;
    this.location = location;
    this.date = date;
    this.boundWiths = boundWiths;
    this.call = call;
    this.itemSummary = itemSummary;
    this.orderNote = openOrderNote;
    this.avail = avail;
    this.circ = circ;
    this.online = online;
    this.active = active;
  }

  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  public boolean noItemsAvailability() {
	  if (this.itemSummary != null) return false;
	  if (this.online != null && this.online) return false;
	  this.avail = this.orderNote == null;
	  return true;
  }

  @JsonIgnore
  public Set<String> getLocationFacetValues() {
    Set<String> facetValues = new LinkedHashSet<>();
    if (this.location != null)
      facetValues.addAll(
          Locations.facetValues(this.location, this.call, (this.notes == null) ? null : String.join(" ", this.notes)));
    else return facetValues;

    if (this.itemSummary != null && this.itemSummary.tempLocs != null)
      for (ItemReference ir : this.itemSummary.tempLocs)
        if (ir.location != null) {
          Set<String> tempLocFacetValues = Locations.facetValues(ir.location, this.call,
              (this.notes == null) ? null : String.join(" ", this.notes));
          if (tempLocFacetValues != null)
            facetValues.addAll(tempLocFacetValues);
        }
    return facetValues;
  }

  public boolean summarizeItemAvailability( TreeSet<Item> items ) {
    int itemCount = 0;
    boolean discharged = false;
    List<ItemReference> unavails = new ArrayList<>();
    List<ItemReference> returned = new ArrayList<>();
    List<ItemReference> tempLocs = null;
    Set<Location> itemLocations = new HashSet<>();
/*TODO boundWiths    if (this.boundWiths != null)
      for (Entry<Integer,BoundWith> bw : this.boundWiths.entrySet()) {
        itemCount++;
        if (! bw.getValue().status.status.equals("Available"))
          unavails.add(new ItemReference(bw.getKey(),true,bw.getValue().thisEnum,null,null,null,null));
      }*/
    boolean circ = false;
    for (Item item : items) {
      itemCount++;
      itemLocations.add(item.location);
      if (! item.status.status.equals("Available") ) {
        unavails.add(new ItemReference(item.id,null,item.concatEnum(),item.status,null,item.holds,item.recalls));
      } else if (item.status.returned != null) {
        discharged = true;
        returned.add(new ItemReference(item.id,null,item.concatEnum(),item.status,null,null,null));
      }
      if (item.loanType != null && ! item.loanType.name.equals(ExpectedLoanType.NOCIRC.name()))
        circ = true;
    }
    if (itemCount == 0)
      return false;
    this.circ = circ;
    if (itemLocations.size() == 1) {
      Location itemLoc = itemLocations.iterator().next();
      if (! itemLoc.equals(this.location))
        this.location = itemLoc;
    } else {
      tempLocs = new ArrayList<>();
      for (Item i : items)
        if (! i.location.equals(this.location))
          tempLocs.add(new ItemReference(i.id,null,i.concatEnum(),null,i.location,i.holds,i.recalls));
          
    }
    this.itemSummary = new HoldingsItemSummary(
        itemCount,
        (itemCount-unavails.size()==0)?null:itemCount-unavails.size(),
        (unavails.size() == 0)?null:unavails,
        tempLocs,
        (returned.size() == 0)?null:returned);
    return discharged;
  }

  /**
   * Any time a comma is followed by a character that is not a space, a
   * space will be inserted.
   */
  @JsonIgnore private static Pattern commaFollowedByNonSpace = null;
  private static String insertSpaceAfterCommas( String s ) {
    if (commaFollowedByNonSpace == null)
      commaFollowedByNonSpace = Pattern.compile(",([^\\s])");
    return commaFollowedByNonSpace.matcher(s).replaceAll(", $1");
  }

  public static class HoldingsItemSummary {
    @JsonProperty("count")    public final int itemCount;
    @JsonProperty("avail")    public final Integer availItemCount;
    @JsonProperty("unavail")  public final List<ItemReference> unavail;
    @JsonProperty("tempLoc")  public final List<ItemReference> tempLocs;
    @JsonProperty("returned") public final List<ItemReference> returned;

    @JsonCreator
    public HoldingsItemSummary(
        @JsonProperty("count")    int itemCount,
        @JsonProperty("avail")    Integer availItemCount,
        @JsonProperty("unavail")  List<ItemReference> unavail,
        @JsonProperty("tempLoc")  List<ItemReference> tempLocs,
        @JsonProperty("returned") List<ItemReference> returned) {
      this.itemCount = itemCount;
      this.availItemCount = availItemCount;
      this.unavail = unavail;
      this.tempLocs = tempLocs;
      this.returned = returned;
    }
  }

  public static class Link {
    @JsonProperty("url")          public final String url;
    @JsonProperty("description")  public final String desc;
    @JsonProperty("ssid")         public final String ssid;
    @JsonProperty("dbcode")       public final String dbcode;
    @JsonProperty("providercode") public final String providercode;
    @JsonProperty("titleid")      public final String titleid;
    @JsonProperty("users")        public final Integer users;

    public Link (
        @JsonProperty("url")          String url,
        @JsonProperty("description")  String desc,
        @JsonProperty("ssid")         String ssid,
        @JsonProperty("dbcode")       String dbcode,
        @JsonProperty("providercode") String providercode,
        @JsonProperty("titleid")      String titleid,
        @JsonProperty("users")        Integer users) {
      this.url  = url;
      this.desc = desc;
      this.ssid = ssid;
      this.dbcode = dbcode;
      this.providercode = providercode;
      this.titleid = titleid;
      this.users = users;
    }

    public static Link fromJson( String json ) throws IOException {
      return mapper.readValue(json, Link.class);
    }
  }
}