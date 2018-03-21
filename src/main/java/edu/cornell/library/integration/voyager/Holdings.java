package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Holdings {

  private static final String holdingsByBibIdQuery =
      "SELECT *"+
      "  FROM bib_mfhd, mfhd_master"+
      " WHERE bib_mfhd.mfhd_id = mfhd_master.mfhd_id"+
      "   AND bib_mfhd.bib_id = ?"+
      "   AND mfhd_master.suppress_in_opac = 'N'"+
      " ORDER BY bib_mfhd.mfhd_id";
  private static final String holdingByHoldingIdQuery =
      "SELECT *"+
      "  FROM bib_mfhd, mfhd_master"+
      " WHERE bib_mfhd.mfhd_id = mfhd_master.mfhd_id"+
      "   AND bib_mfhd.mfhd_id = ?";

  public static void detectChangedHoldings( Connection voyager ) {

  }

  public static void detectChangedOrderStatus( Connection voyager ) {

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
      holdings.put(mfhdId, holding);
    }
    public int size() {
      return holdings.size();
    }
    public Holding get( Integer mfhdId ) {
      return holdings.get(mfhdId);
    }
    public Collection<Holding> values() {
      return this.holdings.values();
    }

    public Set<Integer> getMfhdIds( ) {
      return holdings.keySet();
    }
    public String toJson() throws JsonProcessingException {
      return mapper.writeValueAsString(this.holdings);
    }
  }

}
