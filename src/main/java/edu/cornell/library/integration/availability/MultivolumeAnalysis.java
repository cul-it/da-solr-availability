package edu.cornell.library.integration.availability;

import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;
import edu.cornell.library.integration.voyager.Locations.Location;

/**
 * Analysis, when applied to holdings and items information for a single bib has these goals:<br/>
 * <ol><li>Determine whether the bib is a multi-volume record. If all items are not copies
 *  of the same material, even if the work isn't explicitly multi-volume (e.g. a book and 
 *  accompanying media as separate items to be separately requested), the record should be
 *  considered multi-volume for the purposes of requests. That is, a patron should be able to
 *  choose which item they want</li>
 * <li>Ensure that all "multi-volume" records' items (as previously defined) are enumerated
 *  in the item json. This is particularly a concern in the case of the book with
 *  accompanying material, as the accompanying material will probably be enumerated (e.g.
 *  CDROM), but the actual book will probably not be. In these cases, we use the
 *  holdings description as a placeholder for that enumeration. This might typically be
 *  "1 v.", rather than the "v.1" that would be more likely if the book <i>had</i> been
 *  enumerated, but it serves to have something in a selection drop-down box for the
 *  requesting patron.</li></ol>
 */
public class MultivolumeAnalysis {

  /**
   * Perform multi-volume determination based on holdings and items for a single bib record,
   * returning a multi-volume boolean. In the case of a multi-volume record, the provided
   * items may be modified to supply an enumeration for all items not already having an
   * enumeration, chronology, or year value.
   */
  public static EnumSet<MultiVolFlag> analyze(
      String format, String description, boolean hasDescriptionE, HoldingSet holdings, ItemList items) {
/*
    System.out.println(format);
    System.out.println(description);
    System.out.println(hasDescriptionE);
    System.out.println(holdings.toJson());
    System.out.println(items.toJson());
    System.exit(0);
*/
    EnumSet<MultiVolFlag> flags = EnumSet.noneOf(MultiVolFlag.class);
    Boolean multivol = null;

    // Look for variability of enumeration within a single location
    Map<Location,LocationEnumStats> enumStats = new HashMap<>();
    for (Entry<Integer, TreeSet<Item>> e : items.getItems().entrySet()) {
      for (Item i : e.getValue()) {
        if ( ! enumStats.containsKey(i.location) )
          enumStats.put(i.location, new LocationEnumStats());
        LocationEnumStats stats = enumStats.get(i.location);

        if ( ! stats.diverseEnumFound || ! stats.blankEnumFound ) {
          String enumeration = ((i.enumeration==null)?"":i.enumeration)
              +((i.chron==null)?"":i.chron)+((i.year==null)?"":i.year);
          enumeration = enumeration.replaceAll("c\\.\\d+", "");
          enumeration = enumeration.replaceAll("Bound with", "");
          if (stats.aFoundEnum == null)
            stats.aFoundEnum = enumeration;
          if (! stats.aFoundEnum.equals(enumeration))
            stats.diverseEnumFound = true;

          if (enumeration.equals(""))
            stats.blankEnumFound = true;
          else if (stats.aFoundEnum.equals(""))
            stats.aFoundEnum = enumeration;
        }
      }
    }

    // if we have diverse enumeration within a single loc, it's a multivol
    boolean blankEnum = false;
    boolean nonBlankEnum = false;
    for (Location loc : enumStats.keySet()) {
      LocationEnumStats l = enumStats.get(loc);
      if (l.diverseEnumFound) multivol = true;
      if (l.blankEnumFound) blankEnum = true;
      if ( ! l.aFoundEnum.equals("")) nonBlankEnum = true;
      if (l.blankEnumFound && l.diverseEnumFound)
        flags.add(MultiVolFlag.MAINITEM);
    }

    // Across different locations, diversity only considers enumerations that aren't blank
    if (multivol == null && enumStats.size() > 1) {
      Collection<String> nonBlankEnums = new HashSet<>();
      for (LocationEnumStats stats : enumStats.values())
        if (! stats.aFoundEnum.equals(""))
          if ( ! nonBlankEnums.contains(stats.aFoundEnum))
            nonBlankEnums.add(stats.aFoundEnum);
      // nonBlankEnums differ between locations
      if (nonBlankEnums.size() > 1)
        multivol = true;

      // enumeration is consistent across locations
      else if ( ! blankEnum )
        multivol = false;
    }

    if (multivol == null && blankEnum && nonBlankEnum) {
      // We want to separate the cases where:
      //   1) this is a multivol where one item was accidentally not enumerated.
      //   2) this is a single volume work which is enumerated in one location 
      //               and not the other
      //   3) this is a single volume work with supplementary material, and the
      //               item lacking enumeration is the main item
      boolean descriptionLooksMultivol = doesDescriptionLookMultivol(description);
      if (hasDescriptionE) {
        // this is strong evidence for case 3
        if ( ! descriptionLooksMultivol) {
          // confirm case 3
          multivol = true;
          flags.add(MultiVolFlag.MAINITEM);
        } else {
          // multivol with an e? Not sure here, but concluding case 1
          multivol = true;
          flags.add(MultiVolFlag.SUSPICIOUSENUM);
        }
      } else {
        // Serial title?
        if (format.equals("Journal/Periodical")) {
          //concluding case 1 for now
          multivol = true;
          flags.add(MultiVolFlag.SUSPICIOUSENUM);
        } else  {
          // not known to be a multivol, has no 300e, isn't a serial
          // conclude 2 for now.
          multivol = false;
          flags.add(MultiVolFlag.SUSPICIOUSENUM);
          flags.add(MultiVolFlag.MAINITEM);
        }
      }
    }
    if (multivol != null && multivol)
      flags.add(MultiVolFlag.MULTIVOL);
    if (blankEnum)
      flags.add(MultiVolFlag.BLANKENUM);
    if (nonBlankEnum)
      flags.add(MultiVolFlag.NONBLANKENUM);

    if (flags.contains(MultiVolFlag.MULTIVOL) && flags.contains(MultiVolFlag.BLANKENUM))
      if ( putHoldingsDescriptionInPlaceOfMissingItemEnums( holdings, items ) )
        flags.add(MultiVolFlag.MISSINGHOLDINGSDESC);
    
    return flags;
  }

  private static boolean putHoldingsDescriptionInPlaceOfMissingItemEnums(HoldingSet holdings, ItemList items) {
    boolean missingHoldingsDescriptions = false;
    for (Entry<Integer, TreeSet<Item>> e : items.getItems().entrySet()) {
      int holdingId = e.getKey();

      for (Item i : e.getValue()) {
        if (i.chron == null && i.enumeration == null && i.year == null) {
          List<String> holdingsDesc = holdings.get(holdingId).holdings;
          if (holdingsDesc == null || holdingsDesc.isEmpty()) {
            missingHoldingsDescriptions = true;
            String holdingCallNo = holdings.get(holdingId).call;
            if (holdingCallNo != null)
              i.enumeration = holdingCallNo;
            continue;
          }
          i.enumeration = holdingsDesc.get(0);
        }
        
      }
    }
    return missingHoldingsDescriptions;
  }

  private static Pattern multivolDesc = Pattern.compile("(\\d+)\\D* v\\.");
  private static Pattern singlevolDesc = Pattern.compile("^([^0-9\\-\\[\\]  ] )?p\\.");
  private static boolean doesDescriptionLookMultivol(String description) {

    if (description == null || description.isEmpty()) return false;

    Matcher m = multivolDesc.matcher(description);
    if (m.find()) {
      int c = Integer.valueOf(m.group(1).replaceAll("[^\\d\\-\\.]", ""));
      if (c > 1) return true;
      if (c == 1) return false;
    }
    if (singlevolDesc.matcher(description).find()) return false;

    return false;
  }

  public static enum MultiVolFlag {
    MULTIVOL("multivol_b"),
    BLANKENUM("blankenum_b"),
    NONBLANKENUM("enum_b"),
    MAINITEM("mainitem_b"),
    SUSPICIOUSENUM("suspiciousMultivolEnum_b"),
    MULTIVOLNOHDESC("multivolNoHoldingsDesc_b"),
    MISSINGHOLDINGSDESC("multivolMissingDesc_b");

    private String solrField;
    private MultiVolFlag( String s ) { this.solrField = s; }
    public String getSolrField() { return this.solrField; }
  }

  protected static class LocationEnumStats {
    public String aFoundEnum = null;
    public Boolean blankEnumFound = false;
    public Boolean diverseEnumFound = false;
  }

}
