package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.voyager.Holdings.HoldingsItemSummary;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Locations.Location;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class Holding {

  @JsonProperty("copy")        private final Integer copy;
  @JsonProperty("notes")       private final List<String> notes;
  @JsonProperty("holdings")    private final List<String> holdings;
  @JsonProperty("supplements") private final List<String> supplements;
  @JsonProperty("indexes")     private final List<String> indexes;
  @JsonProperty("location")    private Location location;
  @JsonProperty("call")        public final String call;
  @JsonProperty("date")        public final Integer date;
  @JsonProperty("boundWith")   public final Map<Integer,BoundWith> boundWiths;
  @JsonProperty("items")       public HoldingsItemSummary itemSummary = null;

  @JsonIgnore public MarcRecord record;
  @JsonIgnore static Locations locations = null;
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
          case 'z':
            notes.add(sf.value); break CODE;
          case 't':
            copy = Integer.valueOf(sf.value); break CODE;
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
    this.location = holdingLocation;
    this.boundWiths = (boundWiths.isEmpty())?null:boundWiths;
    this.call = call;
  }

  Holding(
      @JsonProperty("copy")        Integer copy,
      @JsonProperty("notes")       List<String> notes,
      @JsonProperty("holdings")    List<String> holdings,
      @JsonProperty("supplements") List<String> supplements,
      @JsonProperty("indexes")     List<String> indexes,
      @JsonProperty("location")    Location location,
      @JsonProperty("call")        String call,
      @JsonProperty("date")        Integer date,
      @JsonProperty("boundWith")   Map<Integer,BoundWith> boundWiths,
      @JsonProperty("items")       HoldingsItemSummary itemSummary) {
    this.copy = copy;
    this.notes = (notes == null || notes.isEmpty()) ? null : notes;
    this.holdings = (holdings == null || holdings.isEmpty()) ? null : holdings;
    this.supplements = (supplements == null || supplements.isEmpty()) ? null : supplements;
    this.indexes = (indexes == null || indexes.isEmpty()) ? null : indexes;
    this.location = location;
    this.date = date;
    this.boundWiths = boundWiths;
    this.call = call;
    this.itemSummary = itemSummary;
  }

  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  public void summarizeItemAvailability( TreeSet<Item> treeSet ) {
    int itemCount = 0;
    List<ItemReference> unavails = new ArrayList<>();
    List<ItemReference> returned = new ArrayList<>();
    List<ItemReference> tempLocs = null;
    Set<Location> itemLocations = new HashSet<>();
    if (boundWiths != null)
      for (Entry<Integer,BoundWith> bw : boundWiths.entrySet()) {
        itemCount++;
        if (! bw.getValue().status.available)
          unavails.add(new ItemReference(bw.getKey(),true,bw.getValue().thisEnum,null,null));
      }
    for (Item item : treeSet) {
      itemCount++;
      itemLocations.add(item.location);
      if (! item.status.available) {
        item.status.available = null;
        unavails.add(new ItemReference(item.itemId,null,item.enumeration,item.status,null));
      } else if (item.status.codes.values().contains("Discharged")) {
        returned.add(new ItemReference(item.itemId,null,item.enumeration,item.status,null));
      }
    }
    if (itemCount == 0)
      return;
    if (itemLocations.size() == 1) {
      Location itemLoc = itemLocations.iterator().next();
      if (! itemLoc.equals(this.location))
        this.location = itemLoc;
    } else {
      tempLocs = new ArrayList<>();
      for (Item i : treeSet)
        if (! i.location.equals(this.location))
          tempLocs.add(new ItemReference(i.itemId,null,i.enumeration,null,i.location));
          
    }
    this.itemSummary = new HoldingsItemSummary(
        itemCount, (unavails.size() == 0)?null:unavails,
        tempLocs, (returned.size() == 0)?null:returned);
  }
  /**
   * Any time a comma is followed by a character that is not a space, a
   * space will be inserted.
   */
  private static Pattern commaFollowedByNonSpace = null;
  private static String insertSpaceAfterCommas( String s ) {
    if (commaFollowedByNonSpace == null)
      commaFollowedByNonSpace = Pattern.compile(",([^\\s])");
    return commaFollowedByNonSpace.matcher(s).replaceAll(", $1");
  }

}
