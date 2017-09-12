package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.Locations.Location;

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
  static Locations locations = null;

  public static void detectChangedHoldings( Connection voyager ) {

  }

  public static void detectChangedOrderStatus( Connection voyager ) {

  }

  public static List<Holding> retrieveHoldingsByBibId( Connection voyager, int bib_id )
      throws SQLException, IOException, XMLStreamException {
    if (locations == null)
      locations = new Locations( voyager );
    List<Holding> holdings = new ArrayList<>();
    try (PreparedStatement pstmt = voyager.prepareStatement(holdingsByBibIdQuery)) {
      pstmt.setInt(1, bib_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          holdings.add(new Holding(voyager,rs));
      }
    }
    return holdings;
  }

  public static Holding retrieveHoldingsByHoldingId( Connection voyager, int mfhd_id )
      throws SQLException, IOException, XMLStreamException {
    if (locations == null)
      locations = new Locations( voyager );
    try (PreparedStatement pstmt = voyager.prepareStatement(holdingByHoldingIdQuery)) {
      pstmt.setInt(1, mfhd_id);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          return new Holding(voyager,rs);
      }
    }
    return null;
  }

  static ObjectMapper mapper = new ObjectMapper();
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  public static class Holding {

    @JsonProperty("id")        private final int mfhdId;
    @JsonProperty("bibId")     private final int bibId;
    @JsonProperty("copyNum")   private final String copyNum;
    @JsonProperty("notes")     private final List<String> notes;
    @JsonProperty("desc")      private final List<String> desc;
    @JsonProperty("supplDesc") private final List<String> supplDesc;
    @JsonProperty("indexDesc") private final List<String> indexDesc;
    @JsonProperty("location")  private final Location location;
    @JsonProperty("date")      public final Integer date;
    @JsonProperty("boundWith") public final Map<Integer,BoundWith> boundWiths;

    @JsonIgnore public MarcRecord record;

    private Holding(Connection voyager, ResultSet rs) throws SQLException, IOException, XMLStreamException {
      this.mfhdId = rs.getInt("mfhd_id");
      this.bibId = rs.getInt("bib_id");
      this.date = (int)(((rs.getTimestamp("update_date") == null)
          ? rs.getTimestamp("create_date") : rs.getTimestamp("update_date")).getTime()/1000);
      String mrc = DownloadMARC.downloadMrc(voyager, RecordType.HOLDINGS, this.mfhdId);
      this.record = new MarcRecord( RecordType.HOLDINGS, mrc );

      // process data from holdings marc
      final Map<Integer,BoundWith> boundWiths = new HashMap<>();
      Location holdingLocation = null;
      Collection<String> callnos = new HashSet<>();
      List<String> holdingDescs = new ArrayList<>();
      List<String> recentHoldings = new ArrayList<>();
      List<String> supplementalHoldings = new ArrayList<>();
      List<String> indexHoldings = new ArrayList<>();
      List<String> notes = new ArrayList<>();
      String copyNum = null;
      Collection<DataField> sortedFields = this.record.matchSortAndFlattenDataFields();
      for( DataField f: sortedFields ) {
        String callno = null;
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
              // multiple subfield ‡h entries in one field, the callno will be overwritten
              // and not duplicated in the call number array.
              callno = f.concatenateSpecificSubfields("hijklm"); break CODE;
            case 'z':
              notes.add(sf.value); break CODE;
            case 't':
              copyNum = sf.value; break CODE;
            }
          }
          break;
        case "866":
          if (f.ind1.equals(' ') && f.ind2.equals(' '))
            recentHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
          else
            holdingDescs.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
          break;
        case "867":
          supplementalHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
          break;
        case "868":
          indexHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
          break;
        case "876":
          BoundWith b = BoundWith.from876Field(voyager, f);
          if (b != null) boundWiths.put(b.masterItemId,b);
        }
        if (callno != null)
          callnos.add(callno);
      }
      this.copyNum = copyNum;
      this.notes = notes;
      this.desc = holdingDescs;
      this.supplDesc = supplementalHoldings;
      this.indexDesc = indexHoldings;
      this.location = holdingLocation;
      this.boundWiths = (boundWiths.isEmpty())?null:boundWiths;
    }

    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this);
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
}
