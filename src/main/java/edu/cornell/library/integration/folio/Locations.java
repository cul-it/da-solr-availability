package edu.cornell.library.integration.folio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Give access to location data by code or number. Will load loaded into memory
 * from the Voyager database on the first instantiation of the class.
 */
public final class Locations {

  /**
   * Give access to location data by code or number. If this is the first time
   * Locations has been instantiated in the current process, instantiation will
   * attempt to connect to Voyager to retrieve and index the data. Otherwise,
   * the instance will simply give access to the already loaded data.
   * @throws IOException 
   */
  public Locations(final OkapiClient okapi) throws IOException {

    if (_byCode.isEmpty())
      populateLocationMaps(okapi);
  }

  /**
   * Give access to location data by code or number. This must not be the first
   * instantiation of a Locations() object in the current process, or an IOException
   * will be thrown.
   * @throws IOException 
   */
  public Locations() throws IOException {

    if (_byCode.isEmpty())
      throw new IOException("Locations must be instantiated before this point");
  }

  /**
   * Retrieve Location object based on <b>code</b>. The value will already have
   * been loaded into memory. The method cannot be called statically to ensure
   * that the instantiation has been able to load the data.
   * 
   * @param code
   * @return Location
   */
  @SuppressWarnings("static-method")
  public final Location getByCode(final String code) {
    if (_byCode.containsKey(code))
      return _byCode.get(code);
    return null;
  }

  /**
   * Retrieve Location object based on <b>uuid</b>. The value will already
   * have been loaded into memory. The method cannot be called statically to
   * ensure that the instantiation has been able to load the data.
   * 
   * @param uuid
   * @return Location
   */
  @SuppressWarnings("static-method")
  public final Location getByUuid(final String uuid) {
    if (_byUuid.containsKey(uuid))
      return _byUuid.get(uuid);
    return null;
  }

  public static Set<String> facetValues( final Location l, String call, String holdingNote ) {
    String lcCall = (call == null)        ? null : call.toLowerCase();
    String lcNote = (holdingNote == null) ? null : holdingNote.toLowerCase();
    if ( _facetsByLocation.containsKey(l) )
      for (FacetMapRule rule : _facetsByLocation.get(l))
        if (( rule.call == null        || (lcCall != null && lcCall.contains( rule.call )) ) &&
            ( rule.holdingNote == null || (lcNote != null && lcNote.contains( rule.holdingNote )) ) ) {
          if ( ! rule.suppress )
            return rule.facetValues;
          return new LinkedHashSet<>();
        }
    System.out.println("Failed to map facet location for: "+l);
    return new LinkedHashSet<>();
  }


  /**
   * Structure containing values relating to holdings location. <b>Name</b> and <b>library</b>
   * may potentially be null.<br/><br/>
   * <b>Fields</b><hr/>
   * <dl>
   *  <dt>code</dt><dd>Location code, as appears in holdings 852$b, e.g. "fine,res"</dd>
   *  <dt>number</dt><dd>Location number, as appears in item record, e.g. 33</dd>
   *  <dt>name</dt><dd>Location name, e.g. "Fine Arts Library Reserve"</dd>
   *  <dt>library</dt><dd>Unit Library name, e.g. "Fine Arts Library"</dd>
   * </dl>
   * <b>Methods</b><hr/>
   * <dl>
   *  <dt>toString()</dt><dd>Returns display value of Location, primarily for diagnostic use,
   *     e.g. "code: fine,res; number: 33; name: Fine Arts Library Reserve; library: Fine Arts Library"</dd>
   *  <dt>equals( Location other )</dt><dd>returns <b>true</b> if this.number == other.number; else <b>false</b></dd>
   *  <dt>compareTo( Location other )</dt><dd>returns this.number.compareTo(other.number)</dd>
   * </dl>
   */
  public static class Location implements Comparable<Location> {
    public final String code;
    public final String name;
    public final String library;
    public final String hoursCode;
    public final String id;
    public final String primaryServicePoint;

    /**
     * @return
     *  Display value of Location, primarily for diagnostic use,
     *     e.g. "code: fine,res; number: 33; name: Fine Arts Library Reserve; library: Fine Arts Library"
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("code: ").append(this.code);
      sb.append("; name: ").append(this.name);
      sb.append("; library: ").append(this.library);
      if (this.hoursCode != null)
        sb.append("; hoursCode: ").append(this.hoursCode);
      return sb.toString();
    }

    /**
     * @param other
     * @return <b>true</b> if this.number == other.number; else <b>false</b>
     */
    @Override
    public boolean equals( final Object other ) {
      if (other == null) return false;
      if (! this.getClass().equals(other.getClass())) return false;
      return (((Location)other).code.equals(this.code));
    }
    @Override
    public int hashCode() {
      return this.code.hashCode();
    }

    /**
     * @param other
     * @return this.number.compareTo(other.number)
     */
    @Override
    public int compareTo(final Location other) {
      return this.code.compareTo(other.code);
    }

    @JsonCreator
    Location(
        @JsonProperty("code")      String code,
        @JsonProperty("name")      String name,
        @JsonProperty("library")   String library,
        @JsonProperty("hoursCode") String hoursCode,
        @JsonProperty("id")        String id,
        @JsonProperty("primaryServicePoint")
                                   String primaryServicePoint
        ) {
      this.code = code;
      this.name = name.trim();
      this.library = library;
      this.hoursCode = hoursCode;
      this.id = id;
      this.primaryServicePoint = primaryServicePoint;
    }
  }

  public static enum Sort { CODE, UUID }

  public static Collection<Location> allLocations(Sort sortOrder) {
    switch (sortOrder) {
    case CODE: return _byCode.values();
    case UUID: return _byUuid.values();
    }
    return null;
  }

  // PRIVATE RESOURCES

  private static final Map<String, Location> _byCode = new TreeMap<>();
  private static final Map<String, Location> _byUuid = new TreeMap<>();
  private static final Map<Location, List<FacetMapRule>> _facetsByLocation = new TreeMap<>();

  private static void populateLocationMaps(final OkapiClient okapi) throws IOException {
    Map<String,Map<String,String>> libraryPatterns = loadPatternMap("library_names.txt");
    List<FacetMapRule> facetPatterns = loadFacetPatternMap("LocationFacetMapping.txt");

    ReferenceData libraries = new ReferenceData( okapi, "/location-units/libraries", "name");
    List<Map<String,Object>> okapiLocs = okapi.queryAsList("/locations", null, 500);
    for (Map<String,Object> okapiLoc : okapiLocs) {
      String name = (String)okapiLoc.get("discoveryDisplayName");
      if (name == null)
        name = (String)okapiLoc.get("name");
      Map<String,String> libraryDetails = getLibrary(name, libraryPatterns);
      String libraryName = libraries.getName((String)okapiLoc.get("libraryId"));
      String id = (String)okapiLoc.get("id");
      String primaryServicePoint = (String)okapiLoc.get("primaryServicePoint");
      String hoursCode   = (libraryDetails==null)?null:libraryDetails.values().iterator().next();
      Location l = new Location((String)okapiLoc.get("code"), name, libraryName, hoursCode,id,primaryServicePoint);
      _byCode.put(l.code, l);
      _byUuid.put((String)okapiLoc.get("id"), l);
      List<FacetMapRule> locationFacetRules = new ArrayList<>();
      for (FacetMapRule rule : facetPatterns)
        if (rule.displayName.equals(l.name))
            locationFacetRules.add(rule);
      _facetsByLocation.put(l, locationFacetRules);
    }
  }

  private static Map<String,String> getLibrary(String name, Map<String,Map<String,String>> libraryPatterns) {
    if (name == null)
      return null;
    String lcName = name.toLowerCase();
    Iterator<String> i = libraryPatterns.keySet().iterator();
    while (i.hasNext()) {
      String pattern = i.next();
      if (lcName.contains(pattern))
        return libraryPatterns.get(pattern);
    }
    return null;
  }

  private static Map<String,Map<String,String>> loadPatternMap(String filename) {

    Map<String,Map<String,String>> patternMap = new LinkedHashMap<>();
    try (BufferedReader in = new BufferedReader(new InputStreamReader( 
        Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)))) {
      String site;
      while ((site = in.readLine()) != null) {
        String[] parts = site.split("\\t", 3);
        if (parts.length < 2)
          continue;
        Map<String,String> l = new TreeMap<>();
        l.put(parts[1],(parts.length == 2)?null:parts[2]);
        patternMap.put(parts[0].toLowerCase(), l);
      }
    } catch (IOException e) {
      System.out.println("Couldn't read config file for site identifications.");
      e.printStackTrace();
      System.exit(1);
    }
    return patternMap;
  }

  private static List<FacetMapRule> loadFacetPatternMap(String filename) {

    List<FacetMapRule> patternMap = new ArrayList<>();
    try (BufferedReader in = new BufferedReader(new InputStreamReader( 
        Thread.currentThread().getContextClassLoader().getResourceAsStream(filename), StandardCharsets.UTF_8
        ))) {
      String site;
      while ((site = in.readLine()) != null) {
        String[] parts = site.split("\\t", 5);
        if (parts.length < 5)
          continue;
        FacetMapRule rule = new FacetMapRule(
            parts[0],parts[1].contains("X"),parts[2].toLowerCase(),parts[3].toLowerCase(),parts[4]);
        patternMap.add(rule);
      }
    } catch (IOException e) {
      System.out.println("Couldn't read config file for site identifications.");
      e.printStackTrace();
      System.exit(1);
    }
    return patternMap;
  }

  private static class FacetMapRule {

    final String displayName;
    final Boolean suppress;
    final String call;
    final String holdingNote;
    final Set<String> facetValues;

    @Override
    public String toString() {
      return String.format("%s (%s:%s) => %s", this.displayName, ((this.call == null)? "" : this.call),
          ((this.holdingNote == null)? "" : this.holdingNote),   String.join(" ** ", this.facetValues) );
    }
    public FacetMapRule( String displayName, Boolean suppress, String call, String holdingNote, String facetValue) {

      this.displayName = displayName.trim().replaceAll("\"", "");
      this.suppress    = suppress;
      this.call  = (call.isEmpty()) ? null : call;
      this.holdingNote = (holdingNote.isEmpty()) ? null : holdingNote;

      List<String> parts = Arrays.asList(facetValue.replaceAll("\"","").split(" > "));
      this.facetValues = new LinkedHashSet<>();
      List<String> partsToJoin = new ArrayList<>();
      for (int i = 0; i < parts.size(); i++) {
        partsToJoin.add(parts.get(i));
        this.facetValues.add( String.join(" > ",partsToJoin));        
      }
    }
  }

  
}
