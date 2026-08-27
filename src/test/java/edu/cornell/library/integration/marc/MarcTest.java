package edu.cornell.library.integration.marc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MarcTest {

  String inputJson = "{ \"fields\" : [ "
      + "{\"001\":\"15001714\" }, "
      + "{\"008\":\"180307s2018    my           f000 1 may  \" }, "
      + "{\"005\":\"20250904155724.6\" }, "
      + "{\"010\":{\"ind1\":\" \",\"ind2\":\" \", \"subfields\" : [ { \"a\" : \"  2018318116\" } ] } }, "
      // ...
      + "{\"040\":{\"ind1\":\" \",\"ind2\":\" \", \"subfields\" : [ { \"a\" : \"DLC\" }, { \"b\" : \"eng\" }, "
      +     "{ \"e\" : \"rda\" }, { \"c\" : \"DLC\" }, { \"d\" : \"OCLCO\" }, { \"d\" : \"EYM\" }, "
      +     "{ \"d\" : \"OCLCO\" }, { \"d\" : \"OCLCF\" }, { \"d\" : \"COO\" } ] } }, "
      + "{\"042\":{\"ind1\":\" \",\"ind2\":\" \", \"subfields\" : [ { \"a\" : \"lcode\" } ] } }, "
      + "{\"050\":{\"ind1\":\" \",\"ind2\":\"4\", \"subfields\" : [ { \"a\" : \"GR315\" }, "
      +     "{ \"b\" : \".A64 2018\" } ] } }, "
      + "{\"100\":{\"ind1\":\"0\",\"ind2\":\" \", \"subfields\" : [ { \"a\" : \"Angela Engkuan,\" }, "
      +     "{ \"e\" : \"author.\" } ] } }, "
      + "{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\", \"subfields\" : [ { \"a\" : \"Magis sang manuk bura :\" }, "
      +     "{ \"b\" : \"kumpulan cerita rakyat /\" }, { \"c\" : \"Angela Engkuan.\" } ] } }, "
      // ...
      + "{\"650\":{\"ind1\":\" \",\"ind2\":\"0\", \"subfields\" : [ { \"a\" : \"Folk literature, Malay.\" } ] } }, "
      + "{\"650\":{\"ind1\":\" \",\"ind2\":\"7\", \"subfields\" : [ { \"a\" : \"Folk literature, Malay.\" }, "
      +     "{ \"2\" : \"fast\" }, { \"0\" : \"(OCoLC)fst00929161\" } ] } }, "
      + "{\"710\":{\"ind1\":\"2\",\"ind2\":\" \", \"subfields\" : [ { \"a\" : \"Dewan Bahasa dan Pustaka,\" }, "
      +     "{ \"e\" : \"publisher.\" } ] } }, "
      // ...
      + "{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\", \"subfields\" : [ { \"s\":\"6a051510-5ead-459b-8202-0c8662a34860\" },"
      +     "{ \"i\" : \"e872f3a7-06d4-4e5e-a68f-bc699757fe17\" } ] } } ], "
      + "\"leader\" : \"01341cam a2200373 i 4500\" }";
  String expectedHumanReadable =
      "000    01341cam a2200373 i 4500\n"+
      "001    15001714\n"+
      "008    180307s2018    my           f000 1 may  \n"+
      "005    20250904155724.6\n"+
      "010    ‡a   2018318116\n"+
      "040    ‡a DLC ‡b eng ‡e rda ‡c DLC ‡d OCLCO ‡d EYM ‡d OCLCO ‡d OCLCF ‡d COO\n"+
      "042    ‡a lcode\n"+
      "050  4 ‡a GR315 ‡b .A64 2018\n"+
      "100 0  ‡a Angela Engkuan, ‡e author.\n"+
      "245 10 ‡a Magis sang manuk bura : ‡b kumpulan cerita rakyat / ‡c Angela Engkuan.\n"+
      "650  0 ‡a Folk literature, Malay.\n"+
      "650  7 ‡a Folk literature, Malay. ‡2 fast ‡0 (OCoLC)fst00929161\n"+
      "710 2  ‡a Dewan Bahasa dan Pustaka, ‡e publisher.\n"+
      "999 ff ‡s 6a051510-5ead-459b-8202-0c8662a34860 ‡i e872f3a7-06d4-4e5e-a68f-bc699757fe17\n";

  @Test
  public void jsonMarcToMarcRecordTest () throws JsonMappingException, JsonProcessingException  {
    MarcRecord r = MarcRecord.fromJson(inputJson);
    assertEquals(expectedHumanReadable, r.toString());
  }

  @Test
  public void jsonMarcRoundTripTest () throws JsonMappingException, JsonProcessingException {
    MarcRecord r = MarcRecord.fromJson(inputJson);
    String outputJson = r.toJson();
    String expectedGeneratedJson =
        "{\"parsedRecord\":{\"content\":{\"leader\":\"01341cam a2200373 i 4500\","
        + "\"fields\":[{\"001\":\"15001714\"},{\"008\":\"180307s2018    my           f000 1 may  \"},"
        + "{\"005\":\"20250904155724.6\"},"
        + "{\"010\":{\"ind2\":\" \",\"ind1\":\" \",\"subfields\":[{\"a\":\"  2018318116\"}]}},"
        + "{\"040\":{\"ind2\":\" \",\"ind1\":\" \",\"subfields\":[{\"a\":\"DLC\"},{\"b\":\"eng\"},{\"e\":\"rda\"},"
        + "{\"c\":\"DLC\"},{\"d\":\"OCLCO\"},{\"d\":\"EYM\"},{\"d\":\"OCLCO\"},{\"d\":\"OCLCF\"},{\"d\":\"COO\"}]}},"
        + "{\"042\":{\"ind2\":\" \",\"ind1\":\" \",\"subfields\":[{\"a\":\"lcode\"}]}},"
        + "{\"050\":{\"ind2\":\"4\",\"ind1\":\" \",\"subfields\":[{\"a\":\"GR315\"},{\"b\":\".A64 2018\"}]}},"
        + "{\"100\":{\"ind2\":\" \",\"ind1\":\"0\",\"subfields\":[{\"a\":\"Angela Engkuan,\"},{\"e\":\"author.\"}]}},"
        + "{\"245\":{\"ind2\":\"0\",\"ind1\":\"1\",\"subfields\":[{\"a\":\"Magis sang manuk bura :\"},"
        + "{\"b\":\"kumpulan cerita rakyat /\"},{\"c\":\"Angela Engkuan.\"}]}},"
        + "{\"650\":{\"ind2\":\"0\",\"ind1\":\" \",\"subfields\":[{\"a\":\"Folk literature, Malay.\"}]}},"
        + "{\"650\":{\"ind2\":\"7\",\"ind1\":\" \",\"subfields\":[{\"a\":\"Folk literature, Malay.\"},{\"2\":\"fast\"},"
        + "{\"0\":\"(OCoLC)fst00929161\"}]}},{\"710\":{\"ind2\":\" \",\"ind1\":\"2\",\"subfields\":["
        + "{\"a\":\"Dewan Bahasa dan Pustaka,\"},{\"e\":\"publisher.\"}]}},{\"999\":"
        + "{\"ind2\":\"f\",\"ind1\":\"f\",\"subfields\":[{\"s\":\"6a051510-5ead-459b-8202-0c8662a34860\"},"
        + "{\"i\":\"e872f3a7-06d4-4e5e-a68f-bc699757fe17\"}]}}]}}}";
    assertEquals(expectedGeneratedJson, outputJson);
    Map<String,Object> parsedResults = mapper.readValue(outputJson, Map.class);
    assertTrue ( parsedResults.containsKey("parsedRecord") );
    Map<String,Object> parsedRecord = (Map<String,Object>) parsedResults.get("parsedRecord");
    assertTrue( parsedRecord.containsKey("content") );
    MarcRecord r2 = MarcRecord.fromJson(mapper.writeValueAsString( (Map<String,Object>)parsedRecord.get("content") ));
    assertEquals(expectedHumanReadable, r2.toString());
  }
  private static ObjectMapper mapper = new ObjectMapper();
}
