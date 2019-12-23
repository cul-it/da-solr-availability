package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Locations.Location;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class Holding {

  @JsonProperty("copy")        public final Integer copy;
  @JsonProperty("notes")       public final List<String> notes;
  @JsonProperty("holdings")    public final List<String> holdings;
  @JsonProperty("supplements") public final List<String> supplements;
  @JsonProperty("indexes")     public final List<String> indexes;
  @JsonProperty("recents")     public List<String> recentIssues = null;
  @JsonProperty("location")    public Location location;
  @JsonProperty("call")        public String call;
  @JsonProperty("boundWith")   public Map<Integer,BoundWith> boundWiths;
  @JsonProperty("items")       public HoldingsItemSummary itemSummary = null;
  @JsonProperty("order")       public String orderNote = null;
  @JsonProperty("avail")       public Boolean avail = null;
  @JsonProperty("circ")        public Boolean circ = null;
  @JsonProperty("online")      public Boolean online = null;
  @JsonProperty("date")        public final Integer date;
  @JsonProperty("links")       public List<Link> links = null;


  @JsonIgnore public List<String> donors = null;
  @JsonIgnore public String callNumberSuffix = null;
  @JsonIgnore public MarcRecord record;
  @JsonIgnore public static Locations locations = null;
  @JsonIgnore static ObjectMapper mapper = new ObjectMapper();


  Holding(Connection voyager, ResultSet rs) throws SQLException, IOException, XMLStreamException {
    if (locations == null)
      locations = new Locations( voyager );

    this.date = (int)(((rs.getTimestamp("update_date") == null)
        ? rs.getTimestamp("create_date") : rs.getTimestamp("update_date")).getTime()/1000);
    String mrc = DownloadMARC.downloadMrc(voyager, RecordType.HOLDINGS, rs.getInt("mfhd_id"));
    this.record = new MarcRecord( RecordType.HOLDINGS, mrc );

    // process data from holdings marc
    final Map<Integer,BoundWith> boundWiths = new HashMap<>();
    Location holdingLocation = null;
    String call = null;
    List<String> holdings = new ArrayList<>();
    List<String> recent = new ArrayList<>();
    List<String> supplements = new ArrayList<>();
    List<String> indexes = new ArrayList<>();
    List<String> notes = new ArrayList<>();
    Integer copy = null;
    Collection<DataField> sortedFields = this.record.matchSortAndFlattenDataFields();
    for( DataField f: sortedFields ) {
      switch (f.tag) {
      case "506":
        // restrictions on access note
        notes.add(f.concatenateSpecificSubfields("ancdefu3"));
        break;
      case "541":
        // immediate source of acquisition note
        if ( f.ind1.equals('1') ) { // 1 - Not private
          if ( this.donors == null )
            this.donors = new ArrayList<>();
          this.donors.add(f.concatenateSpecificSubfields("3a"));
        }
        break;
      case "561":
        // ownership and custodial history
        if (! f.ind1.equals('0')) // '0' = private
          notes.add(f.concatenateSpecificSubfields("au3"));
        break;
      case "562":
        // copy and version identification note
        notes.add(f.concatenateSpecificSubfields("abcde3"));
        break;
      case "843":
        // reproduction note
        notes.add(f.concatenateSpecificSubfields("abcdefmn3"));
        break;
      case "845":
        // terms governing use and reproduction note
        notes.add(f.concatenateSpecificSubfields("abcdu3"));
        break;
      case "852":
        for (Subfield sf: f.subfields) {
          CODE: switch (sf.code) {
          case 'b':
            Location l = locations.getByCode(sf.value.trim());
            if (l != null)
              holdingLocation = l;
            else
              System.out.println("location not identified for code='"+sf.value+"'.");
            break CODE;
          case 'h':
            // If there is a subfield ‡h, then there is a call number. So we will record
            // a concatenation of all the call number fields. If there are (erroneously)
            // multiple subfield ‡h entries in one field, the call will be overwritten
            // and not duplicated in the call number array.
            call = f.concatenateSpecificSubfields("hijklm"); break CODE;
          case 'm':
            this.callNumberSuffix = f.concatenateSpecificSubfields("m"); break CODE;
          case 'z':
            notes.add(sf.value); break CODE;
          case 't':
            try { 
              copy = Integer.valueOf(sf.value);
            } catch ( @SuppressWarnings("unused") NumberFormatException e ) {
              System.out.println("Holdings copy number is invalid. h"+this.record.id+" ("+sf.value+")");
            }
            break CODE;
          }
        }
        break;
      case "866":
        if (f.ind1.equals(' ') && f.ind2.equals(' '))
          recent.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
        else
          holdings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
        break;
      case "867":
        supplements.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
        break;
      case "868":
        indexes.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
        break;
      case "876":
        BoundWith b = BoundWith.from876Field(voyager, f);
        if (b != null) boundWiths.put(b.masterItemId,b);
      }
    }
    this.copy = copy;
    this.notes = (notes.isEmpty()) ? null : notes;
    this.holdings = (holdings.isEmpty()) ? null : holdings;
    this.supplements = (supplements.isEmpty()) ? null : supplements;
    this.indexes = (indexes.isEmpty()) ? null : indexes;
    if (holdingLocation != null && holdingLocation.equals(locations.getByCode("serv,remo")))
      this.online = true;
    else
      this.location = holdingLocation;
    this.boundWiths = (boundWiths.isEmpty())?null:boundWiths;
    if ( call != null )
      call =  call.replaceAll("[Nn]o [Cc]all [Nn]umber\\.*", "").trim();
    if ( holdingLocation == null || call == null || call.isEmpty() )
      this.call = null;
    else
      this.call = call;
  }

  Holding(
      @JsonProperty("copy")        Integer copy,
      @JsonProperty("notes")       List<String> notes,
      @JsonProperty("holdings")    List<String> holdings,
      @JsonProperty("supplements") List<String> supplements,
      @JsonProperty("indexes")     List<String> indexes,
      @JsonProperty("recents")     List<String> recentIssues,
      @JsonProperty("location")    Location location,
      @JsonProperty("call")        String call,
      @JsonProperty("avail")       Boolean avail,
      @JsonProperty("circ")        Boolean circ,
      @JsonProperty("online")      Boolean online,
      @JsonProperty("date")        Integer date,
      @JsonProperty("boundWith")   Map<Integer,BoundWith> boundWiths,
      @JsonProperty("items")       HoldingsItemSummary itemSummary,
      @JsonProperty("order")       String openOrderNote) {
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
    if (this.boundWiths != null)
      for (Entry<Integer,BoundWith> bw : this.boundWiths.entrySet()) {
        itemCount++;
        if (! bw.getValue().status.available)
          unavails.add(new ItemReference(bw.getKey(),true,bw.getValue().thisEnum,null,null,null,null));
      }
    boolean circ = false;
    for (Item item : items) {
      itemCount++;
      itemLocations.add(item.location);
      if (! item.status.available // || (this.call != null && this.call.matches(".*In Process.*"))
          ) {
        item.status.available = null;
        unavails.add(new ItemReference(item.itemId,null,item.concatEnum(),item.status,null,item.holds,item.recalls));
      } else if (item.status.code.values().contains("Discharged")) {
        discharged = true;
        returned.add(new ItemReference(item.itemId,null,item.concatEnum(),item.status,null,null,null));
      }
      if (item.type != null && ! item.type.name.equals("nocirc"))
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
          tempLocs.add(new ItemReference(i.itemId,null,i.concatEnum(),null,i.location,i.holds,i.recalls));
          
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

    public Link (
        @JsonProperty("url")          String url,
        @JsonProperty("description")  String desc,
        @JsonProperty("ssid")         String ssid,
        @JsonProperty("dbcode")       String dbcode,
        @JsonProperty("providercode") String providercode ) {
      this.url  = url;
      this.desc = desc;
      this.ssid = ssid;
      this.dbcode = dbcode;
      this.providercode = providercode;
    }

    public static Link fromJson( String json ) throws IOException {
      return mapper.readValue(json, Link.class);
    }
  }
}
