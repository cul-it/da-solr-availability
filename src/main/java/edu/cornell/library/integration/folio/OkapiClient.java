package edu.cornell.library.integration.folio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OkapiClient {

  private final String url;
  private final String token;
  private final String tenant;
  
  protected OkapiClient() {
    this.url = "BOGUS_URL";
    this.token = "BOGUS_TOKEN";
    this.tenant = "BOGUS_TENANT";
  }

  public OkapiClient(Properties prop, String identifier) throws IOException {
    this.url = prop.getProperty("okapiUrl"+identifier);
    this.tenant = prop.getProperty("okapiTenant"+identifier);
    if ( prop.containsKey("okapiToken"+identifier))
      this.token = prop.getProperty("okapiToken"+identifier);
    else {
      this.token = post("/authn/login",
          String.format("{\"username\":\"%s\",\"password\":\"%s\"}",
              prop.getProperty("okapiUser"+identifier),prop.getProperty("okapiPass"+identifier)));
      prop.setProperty("okapiToken"+identifier, this.token);
      System.out.println(this.token);
    }
  }

  public String post(final String endPoint, final String json) throws IOException {

    System.out.println("About to post " + endPoint);

    final URL fullPath = new URL(this.url + endPoint);
    final HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
    c.setRequestProperty("Content-Type", "application/json;charset=utf-8");
    c.setRequestProperty("X-Okapi-Tenant", this.tenant);

    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setDoInput(true);
    final OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
    writer.write(json);
    writer.flush();

    String token = c.getHeaderField("x-okapi-token");

    final StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8"))) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    }
    String response = sb.toString();
    if ( token != null )
      return token;
    return response;
  }

  public String put(final String endPoint, final Map<String, Object> object) throws IOException {
    return put(endPoint, (String) object.get("id"), mapper.writeValueAsString(object));
  }

  public String put(final String endPoint, final String uuid, final String json) throws IOException {

    final HttpURLConnection c = commonConnectionSetup(endPoint + "/" + uuid);
    c.setRequestMethod("PUT");
    c.setDoOutput(true);
    c.setDoInput(true);
    final OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
    writer.write(json);
    writer.flush();
    //      int responseCode = httpConnection.getResponseCode();
    //      if (responseCode != 200)
    //          throw new IOException(httpConnection.getResponseMessage());
    final StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8"))) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    }
    return sb.toString();
  }

  public String delete(final String endPoint, final String uuid) throws IOException {
    final StringBuilder sb = delete(endPoint, uuid, new StringBuilder());
    return sb.toString();
  }

  public String deleteAll(final String endPoint, final boolean verbose) throws IOException {

    return deleteAll(endPoint, null, verbose);
  }

  public String deleteAll(final String endPoint, final String notDeletedQuery, final boolean verbose) throws IOException {

    final StringBuilder sb = new StringBuilder();

    Map<String, Map<String, Object>> existing = queryAsMap(endPoint, notDeletedQuery, null);

    while (!existing.isEmpty()) {
      final String output = existing.keySet().parallelStream().map(uuid -> {
        try {
          return String.format("Deleting %s/%s %s\n", endPoint, uuid, delete(endPoint, uuid));
        } catch (final Exception e) {
          return e.getMessage();
        }
      }).collect(Collectors.joining("\n"));
      if (verbose)
        System.out.println(output);
      existing = queryAsMap(endPoint, notDeletedQuery, null);

    }
    return sb.toString();
  }

  public String getRecord(final String endPoint, final String uuid) throws IOException {
    final HttpURLConnection c = commonConnectionSetup(endPoint + "/" + uuid);
    final int responseCode = c.getResponseCode();
    if (responseCode != 200)
      throw new NoSuchObjectException(c.getResponseMessage());

    try (InputStream is = c.getInputStream()) {
      return convertStreamToString(is);
    }
  }

  public List<Map<String, Object>> queryAsList(final String endPoint, final String query, final Integer limit) throws IOException {
    return resultsToList(query(endPoint, query, limit));
  }

  public Map<String, Map<String, Object>> queryAsMap(final String endPoint, final String query, final Integer limit) throws IOException {
    return resultsToMap(query(endPoint, query, limit));
  }

  public String query(final String endPoint, final String query, final Integer limit) throws IOException {
    final StringBuilder sb = new StringBuilder();
    sb.append(endPoint);
    if (query != null) {
      sb.append("?query=");
      sb.append(URLEncoder.encode(query, "UTF-8"));
    }
    if (limit != null) {
      final String limitField = endPoint.startsWith("/perms") ? "length" : "limit";
      sb.append((query == null) ? '?' : '&');
      sb.append(limitField);
      sb.append('=');
      sb.append(limit);
    }
    System.out.println(sb.toString());
    final HttpURLConnection c = commonConnectionSetup(sb.toString());
    final int responseCode = c.getResponseCode();

    if (responseCode != 200) {
      try (InputStream is = c.getErrorStream()) {
        String response = convertStreamToString(is);
        System.out.println(response);
      }
      throw new IOException(c.getResponseMessage());
    }
    try (InputStream is = c.getInputStream()) {
      String response = convertStreamToString(is);
//      System.out.println(response);
      return response;
    }
  }

  public String query(final String endPointQuery) throws IOException {
    final StringBuilder sb = new StringBuilder();
    sb.append(endPointQuery);
    System.out.println(sb.toString());
    final HttpURLConnection c = commonConnectionSetup(sb.toString());
    final int responseCode = c.getResponseCode();
    if (responseCode != 200)
      throw new IOException(c.getResponseMessage());

    try (InputStream is = c.getInputStream()) {
      return convertStreamToString(is);
    }
  }

  static final List<String> notRecordsKeys = Arrays.asList("totalRecords", "resultInfo", "pageSize", "page",
      "totalPages", "meta", "totalRecords", "total");

  @SuppressWarnings("unchecked")
  public static Map<String, Map<String, Object>> resultsToMap(final String readValue)
      throws JsonParseException, JsonMappingException, IOException {
    final Map<String, Map<String, Object>> dataMap = new HashMap<>();

    List<Map<String, Object>> records = null;
    if (readValue.startsWith("[")) {
      records = mapper.readValue(readValue, ArrayList.class);
    } else {
      final Map<String, Object> rawData = mapper.readValue(readValue, Map.class);
      System.out.println(String.join(", ", rawData.keySet()));
      for (final String mainKey : rawData.keySet())
        if (!notRecordsKeys.contains(mainKey)) {
          records = (ArrayList<Map<String, Object>>) rawData.get(mainKey);
          System.out.println(records);
        }
    }
    for (final Map<String, Object> record : records) {
      System.out.println(record.get("name"));
      if (record.containsKey("name") && ((String) record.get("name")).contains("Test License"))
        continue;
      dataMap.put((String) record.get("id"), record);
    }
    return dataMap;
  }

  public static List<Map<String, Object>> resultsToList(final String readValue)
      throws JsonParseException, JsonMappingException, IOException {
    if (readValue.startsWith("["))
      return mapper.readValue(readValue, ArrayList.class);
    final Map<String, Object> rawData = mapper.readValue(readValue, Map.class);
    for (final String mainKey : rawData.keySet()) {
      if (!mainKey.equals("totalRecords") && !mainKey.equals("resultInfo")) {
        @SuppressWarnings("unchecked")
        final
        List<Map<String, Object>> records = (ArrayList<Map<String, Object>>) rawData.get(mainKey);
        return records;
      }
    }
    return null;
  }

  // END OF PUBLIC UTILITIES

  private HttpURLConnection commonConnectionSetup(final String path) throws IOException {
    final URL fullPath = new URL(this.url + path);
    final HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
    c.setRequestProperty("Content-Type", "application/json;charset=utf-8");
    c.setRequestProperty("X-Okapi-Tenant", this.tenant);
    c.setRequestProperty("X-Okapi-Token", this.token);
    return c;

  }

  private StringBuilder delete(final String endPoint, final String uuid, final StringBuilder sb) throws IOException {
    final HttpURLConnection c = commonConnectionSetup(endPoint + "/" + uuid);
    c.setRequestMethod("DELETE");
    //      int responseCode = httpConnection.getResponseCode();
    //      if (responseCode != 200)
    //          throw new IOException(httpConnection.getResponseMessage());
    try (BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8"))) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    }
    return sb;
  }

  private static String convertStreamToString(final java.io.InputStream is) {
    try (java.util.Scanner s = new java.util.Scanner(is)) {
      s.useDelimiter("\\A");
      return s.hasNext() ? s.next() : "";
    }
  }

  protected static ObjectMapper mapper = new ObjectMapper();
}
