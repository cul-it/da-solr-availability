package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Normalizer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;

public class DownloadMARC {


  public static MarcRecord getMarc(Connection inventory, RecordType type, String hrid)
      throws SQLException, IOException {
    if (! type.equals(RecordType.BIBLIOGRAPHIC) )
      throw new IllegalArgumentException( String.format(
          "Folio only contains Bibliographic MARC. Request for (%s, %s) invalid\n",type,hrid));

    String marc = null;
    try ( PreparedStatement bibByInstanceHrid = inventory.prepareStatement(
        "SELECT * FROM bibFolio WHERE instanceHrid = ?") ) {
      bibByInstanceHrid.setString(1, hrid);
      try ( ResultSet rs = bibByInstanceHrid.executeQuery() ) {
        while (rs.next()) marc = rs.getString("content").replaceAll("\\s*\\n\\s*", " ");
      }
    }
    if ( marc != null ) {
      MarcRecord marcRec = jsonToMarcRec( marc );
      return marcRec;
    }
    return null;
  }

  static Pattern modDateP = Pattern.compile("^.*\"updatedDate\" *: *\"([^\"]+)\".*$");

	public static MarcRecord jsonToMarcRec( String marcResponse ) throws IOException {
		Map<String,Object> parsedResults = mapper.readValue(marcResponse, Map.class);
		Map<String,Object> parsedRecord = (Map<String,Object>) parsedResults.get("parsedRecord");
		Map<String,Object> jsonStructure = (Map<String,Object>)parsedRecord.get("content");
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.leader = (String) jsonStructure.get("leader");
		List<Map<String,Object>> fields = (List<Map<String, Object>>) jsonStructure.get("fields");
		int fieldId = 1;
		for ( Map<String,Object> f : fields )
			for ( Entry<String,Object> field : f.entrySet() ) {
				Object fieldValue = field.getValue();
				if ( fieldValue.getClass().equals(String.class) ) {
					rec.controlFields.add(new ControlField(fieldId++,field.getKey(),
							Normalizer.normalize((String) fieldValue,Normalizer.Form.NFC)));
					if (field.getKey().equals("001")) {
						rec.bib_id = (String) fieldValue; rec.id = rec.bib_id;
					}
				} else {
					Map<String,Object> fieldContent = (Map<String, Object>) fieldValue;
					int subfieldId = 1;
					List<Map<String,Object>> subfields = (List<Map<String,Object>>) fieldContent.get("subfields");
					TreeSet<Subfield> processedSubfields = new TreeSet<>();
					for (Map<String,Object> subfield : subfields) {
						if ( subfield.isEmpty() ) continue;
						String code = subfield.keySet().iterator().next();
						processedSubfields.add(new Subfield( subfieldId++, code.charAt(0),
								Normalizer.normalize((String) subfield.get(code),Normalizer.Form.NFC) ));
					}
					rec.dataFields.add(new DataField(fieldId++,field.getKey(),
							((String)fieldContent.get("ind1")).charAt(0),
							((String)fieldContent.get("ind2")).charAt(0),
							processedSubfields
							));
				}
			}
		F: for ( DataField f : rec.dataFields )
			for ( Subfield sf : f.subfields )
				if ( sf.code.equals('6') )
					if (subfield6Pattern.matcher(sf.value).matches()) {
						if (f.tag.equals("880"))
							f.mainTag = sf.value.substring(0,3);
						f.linkNumber = Integer.valueOf(sf.value.substring(4,6));
						continue F;
					}
		if ( parsedResults.containsKey("metadata") ) {
			Map<String,String> metadata = (Map<String,String>)parsedResults.get("metadata");
			rec.moddate = Timestamp.from(isoDT.parse(metadata.get("updatedDate")
					.replace("+0000",""),Instant::from));
		}
		return rec;
	}
	private static ObjectMapper mapper = new ObjectMapper();

	private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("Z"));
	private static Pattern subfield6Pattern = Pattern.compile("[0-9]{3}-[0-9]{2}.*");

}
