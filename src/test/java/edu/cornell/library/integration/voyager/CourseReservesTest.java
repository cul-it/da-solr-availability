package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.voyager.TestUtil.convertStreamToString;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import edu.cornell.library.integration.voyager.Items.ItemList;

public class CourseReservesTest {

  static Map<String,String> examples ;
  static ObjectMapper mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .enable(SerializationFeature.INDENT_OUTPUT);

  // Load test examples using Json mapper config from D&A, and serialize to string
  // the same way we would when populating 
  @BeforeClass
  public static void loadExamples() throws IOException {

    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("items_examples.json")){
      ObjectMapper daMapper = new ObjectMapper();
      Map<String,ItemList> temp = daMapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$" , ""),
          new TypeReference<HashMap<String,ItemList>>() {});
      examples = new HashMap<>();
      for (Entry<String,ItemList> e : temp.entrySet()) {
        examples.put(e.getKey(), e.getValue().toJson());
      }
    }
    
  }

  @Test
  public void availableItem() throws IOException {
    Map<String,Object> output = parseItemsJson(examples.get("expectedJson2282772"),"31924067383830");
    assertEquals("Available",output.get("CURRENT_DUE_DATE"));
    assertEquals("Kroch Library Rare & Manuscripts (Request in advance)",output.get("LOCATION_DISPLAY_NAME"));
    assertEquals("31924067383830",output.get("BARCODE"));
  }

  @Test
  public void checkedOutItem() throws IOException {
    Map<String,Object> output = parseItemsJson(examples.get("expectedJson10013120"),"31924122496684");
    assertEquals("2017-09-23 22:00:00-0400",output.get("CURRENT_DUE_DATE"));
    assertEquals("Olin Library",output.get("LOCATION_DISPLAY_NAME"));
    assertEquals("31924122496684",output.get("BARCODE"));
  }

  @Test
  public void onHoldItem() throws IOException {
    Map<String,Object> output = parseItemsJson(examples.get("expectedJson18847"),"31924009888029");
    assertEquals("",output.get("CURRENT_DUE_DATE"));
    assertEquals("In Transit On Hold",output.get("MESSAGE"));
    assertEquals("31924009888029",output.get("BARCODE"));
  }

  @Test
  public void dischargedMultivolItem() throws IOException {
    Map<String,Object> output = parseItemsJson(examples.get("expectedJsonOnReserve"),"31924124599691");
    assertEquals("",output.get("CURRENT_DUE_DATE"));
    assertEquals("Discharged",output.get("MESSAGE"));
    assertEquals("31924124599691",output.get("BARCODE"));
  }

  @Test
  public void multiVolMixedAvailability() throws IOException {

    Map<String,Object> output = parseItemsJson(examples.get("expectedJsonELECTRICSHEEP"),"31924099417416");
    assertEquals("Available",output.get("CURRENT_DUE_DATE"));
    assertEquals("Olin Library",output.get("LOCATION_DISPLAY_NAME"));
    assertEquals("31924099417416",output.get("BARCODE"));
    assertEquals("3",output.get("COPY_NUMBER"));

    output = parseItemsJson(examples.get("expectedJsonELECTRICSHEEP"),"31924111784553");
    assertEquals("2018-02-01 02:00:00-0500",output.get("CURRENT_DUE_DATE"));
    assertEquals("Olin Library",output.get("LOCATION_DISPLAY_NAME"));
    assertEquals("31924111784553",output.get("BARCODE"));
    assertEquals("1",output.get("COPY_NUMBER"));
  }

  // Method slightly adapted from 
  // https://github.com/cul-it/course-reserves/blob/1149d496afa82d7f7dd570918e529d74c5a51d17/src/main/java/edu/cornell/library/coursereserves/util/SolrUtils.java#L31-L151
  // to recreate the assumptions and processing
  public static Map<String, Object> parseItemsJson(String json, String barcode) throws IOException{
    Map<String, Object> map = new HashMap<>();
    String dateFormat = "yyyy-MM-dd HH:mm:ssZ";

    JsonNode root = mapper.readTree(json);

    Iterator<String> iter = root.fieldNames();
    boolean found = false;
    
    while (iter.hasNext()) {
      String field = iter.next(); 
      JsonNode childNode = root.get(field); 

      for (int i = 0; i < childNode.size(); i++) {
        JsonNode itemNode = childNode.get(i); 
        JsonNode barcodeNode = itemNode.get("barcode");
        //System.out.println(mapper.writeValueAsString(itemNode));

        if (barcodeNode != null && barcode.equals(barcodeNode.asText()) && ! found) {
          found = true;
          map.put("BARCODE", barcodeNode.asText());

            JsonNode copyNode = itemNode.get("copy");
          if (copyNode != null) {
            map.put("COPY_NUMBER", copyNode.asText());
          } else {
            map.put("COPY_NUMBER", "");
            map.put("MESSAGE", "copy number is missing: "+ barcode);
          }
          JsonNode locationNode = itemNode.get("location");
          if (locationNode != null) {
            JsonNode locationNameNode = locationNode.get("name");
            if (locationNameNode != null) {
              map.put("LOCATION_DISPLAY_NAME", locationNameNode.asText());
            } else {
              map.put("LOCATION_DISPLAY_NAME", "DEFAULT LOCATION");
              map.put("MESSAGE", "location node is missing: "+ barcode);
            }
          }

          JsonNode statusNode = itemNode.get("status");
          if (statusNode != null) {
            // System.out.println(mapper.writeValueAsString(statusNode));
            // parse availability by looking at the code 
            JsonNode codeNode = statusNode.get("code");
            if (codeNode != null && codeNode.getNodeType() == JsonNodeType.OBJECT) {
              //logger.info("found code node ");
              Iterator<String> iter2 = codeNode.fieldNames();
              String key = iter2.next(); // there is only one child
              String codeVal = codeNode.get(key).asText();
              // logger.debug(key +" code value: "+ codeVal);
              if (key.equals("1") ) {
                // not charged
                map.put("CURRENT_DUE_DATE", "Available");
              } else if (key.equals("2")) {
                // charged out, get due date
                JsonNode dueNode = statusNode.get("due");
                if (dueNode != null) {
                  String due = getFormattedDate(new Date(dueNode.asLong() * 1000), dateFormat);
                  map.put("CURRENT_DUE_DATE", due);
                } else {
                  // logger.warn("due node is missing");
                  map.put("CURRENT_DUE_DATE", "");
                  map.put("MESSAGE", "due date is missing: "+ barcode);
                }
              } else if (key.equals("3"/*renewed*/)) {
                JsonNode dueNode = statusNode.get("due");
                if (dueNode != null) {
                  String due = getFormattedDate(new Date(dueNode.asLong() * 1000), dateFormat);
                  map.put("CURRENT_DUE_DATE", due);
                } else {
                  // logger.warn("due node is missing");
                  map.put("CURRENT_DUE_DATE", "");
                  map.put("MESSAGE", "due date is missing: "+ barcode);
                }
              } else if (key.equals("4"/*overdue*/)) {
                JsonNode dueNode = statusNode.get("due");
                if (dueNode != null) {
                  String due = getFormattedDate(new Date(dueNode.asLong() * 1000), dateFormat);
                  map.put("CURRENT_DUE_DATE", due);
                } else {
                  // logger.warn("due node is missing");
                  map.put("CURRENT_DUE_DATE", "");
                  map.put("MESSAGE", "due date is missing: "+ barcode);
                } 
              } else { 
                // not available for some other reason
                map.put("CURRENT_DUE_DATE", "");
                map.put("MESSAGE", codeVal);
              }
              
            } 

          } else {
            // logger.warn("status node is missing");
            map.put("CURRENT_DUE_DATE", "");
            map.put("MESSAGE", "status node is missing: "+ barcode);
          }
          return map;

//        else barcode doesn't match...
        }

      }

      if (! found) {
        map.put("CURRENT_DUE_DATE", "");
        map.put("LOCATION_DISPLAY_NAME", "DEFAULT LOCATION");
        map.put("BARCODE", barcode);
      }  
    }
    return map;
  }

  // Lifted from
  // https://github.com/cul-it/course-reserves/blob/1149d496afa82d7f7dd570918e529d74c5a51d17/src/main/java/edu/cornell/library/coursereserves/util/MiscUtils.java#L104-L108
  private static String getFormattedDate(java.util.Date date, String fmt) {
    String formattedDate = new String();
    formattedDate = new SimpleDateFormat(fmt).format(new java.util.Date(date.getTime()));
    return formattedDate;
}

}
