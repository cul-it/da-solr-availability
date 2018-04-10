package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.ItemReference;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class RecordsToSolr {

  public static class Change implements Comparable<Change>{
    public final Type type;
    public final Integer recordId;
    public final String detail;
    public final Timestamp changeDate;
    public final String location;

    public Change (Type type, Integer recordId, String detail, Timestamp changeDate, String location) {
      this.type = type;
      this.recordId = recordId;
      this.detail = detail;
      this.changeDate = changeDate;
      this.location = location;
    }
    public String toString() {
      if (this.location != null)
        return this.type.name()+" "+this.location+" "+this.detail+" "+this.changeDate.toLocalDateTime().format(formatter);
      return this.type.name()+" "+this.detail+" "+this.changeDate.toLocalDateTime().format(formatter);
    }
    public enum Type { BIB, HOLDING, ITEM, CIRC, OTHER };
    private static DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT,FormatStyle.MEDIUM);

    @Override
    public boolean equals( Object o ) {
      if (this == o) return true;
      if (o == null) return false;
      if (this.getClass() != o.getClass()) return false;
      Change other = (Change) o;
      return Objects.equals( this.type,       other.type)
          && Objects.equals( this.changeDate, other.changeDate)
          && Objects.equals( this.detail,     other.detail)
          && Objects.equals( this.location,   other.location);
    }

    @Override
    public int compareTo(Change o) {
      if ( ! this.type.equals( o.type ) ) {
        System.out.println(this.type+":"+o.type);
        return this.type.compareTo( o.type );
      }
      System.out.println("Same type");
      if ( ! this.changeDate.equals( o.changeDate ) )
        return this.changeDate.compareTo( o.changeDate );
      System.out.println("Same timestamp");
      if ( this.detail == null )
        return ( o.detail == null ) ? 0 : -1;
      if ( ! this.detail.equals( o.detail ) )
        return this.detail.compareTo( o.detail );
      System.out.println("Same detail");
      if ( this.location == null )
        return ( o.location == null ) ? 0 : -1;
      if ( ! this.location.equals( o.location ) )
        return this.location.compareTo( o.location );
      System.out.println("Same location");

      return 0;
    }

    @Override
    public int hashCode() {
      return this.toString().hashCode();
    }
  }

  public static void updateBibsInSolr(Connection voyager, Connection inventory, Map<Integer,Set<Change>> changedBibs)
      throws SQLException, IOException, XMLStreamException, SolrServerException {

    try (PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT bib_id, solr_document, title FROM bibRecsSolr"+
        " WHERE bib_id = ?"+
        "   AND active = 1");
        SolrClient solr = new HttpSolrClient( System.getenv("SOLR_URL") )) {
      for (int bibId : changedBibs.keySet()) {
        pstmt.setInt(1, bibId);
        try (ResultSet rs = pstmt.executeQuery()) {
          while ( rs.next() ) {

              SolrInputDocument doc = xml2SolrInputDocument(  rs.getString(2) );
              HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyager,bibId);
              ItemList items = Items.retrieveItemsForHoldings(voyager,holdings);
              if ( holdings.summarizeItemAvailability(items) ) 
                doc.addField("availability_facet", "Returned");
              if ( holdings.applyOpenOrderInformation(voyager,bibId) )
                doc.addField("availability_facet", "On Order");
              if ( holdings.noItemsAvailability() )
                doc.addField("availability_facet", "No Items Print");
              if ( holdings.size() > 0 )
                doc.addField("holdings_json", holdings.toJson());
              if ( items.itemCount() > 0 )
                doc.addField("items_json", items.toJson());
              for ( TreeSet<Item> itemsForHolding : items.getItems().values() )
                for ( Item i : itemsForHolding )
                  if (i.status != null && i.status.shortLoan != null)
                    doc.addField("availability_facet", "Short Loan");
              BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(holdings);
              doc.addField("availability_json", b.toJson());
              if ( ! b.availAt.isEmpty() && ! b.unavailAt.isEmpty() )
                doc.addField("availability_facet", "Avail and Unavail");
              Set<String> locationFacet = holdings.getLocationFacetValues();
              if (locationFacet == null)
                System.out.println("b"+bibId+" location facets are null.");
              else if (! locationFacet.isEmpty()) {
                doc.removeField("location");
                doc.addField("location", locationFacet);
              }
              for ( Integer mfhdId : holdings.getMfhdIds()) {
                Holding h = holdings.get(mfhdId);
                if (h.call != null && h.call.matches(".*In Process.*"))
                  doc.addField("availability_facet","In Process");
                if (h.itemSummary != null && h.itemSummary.unavail != null)
                  for (ItemReference ir : h.itemSummary.unavail)
                    if (ir.status != null && ir.status.code != null) {
                      String status = ir.status.code.values().iterator().next();
                      doc.addField("availability_facet",status);
                    }
              }
              for (int mfhdId : holdings.getMfhdIds()) {
                if (holdings.get(mfhdId).itemSummary != null &&
                    holdings.get(mfhdId).itemSummary.tempLocs != null &&
                    ! holdings.get(mfhdId).itemSummary.tempLocs.isEmpty())
                  doc.addField("availability_facet","Partial Temp Locs");
                if (holdings.get(mfhdId).copy != null)
                  doc.addField("availability_facet", "Copies");
              }
              Set<String> changes = new HashSet<>();
              for (Change c : changedBibs.get(bibId))  changes.add(c.toString());
              
              System.out.println(bibId+" ("+rs.getString(3)+"): "+String.join("; ", changes));
              solr.add( doc );
          }

        }
      }
    }

  }

  public static Map<Integer,Set<Change>> duplicateMap( Map<Integer,Set<Change>> m1 ) {
    Map<Integer,Set<Change>> m2 = new HashMap<>();
    for (Entry<Integer,Set<Change>> e : m1.entrySet())
      m2.put(e.getKey(), new HashSet<>(e.getValue()));
    return m2;
  }

  public static Map<Integer,Set<Change>> eliminateCarryovers( 
      Map<Integer,Set<Change>> newChanges, Map<Integer,Set<Change>> oldChanges) {
    if ( oldChanges.isEmpty() )
      return newChanges;
    System.out.println("Blocking: "+oldChanges.toString());
    List<Integer> bibsToRemove = new ArrayList<>();
    for (Integer newBibId : newChanges.keySet()) {
      if ( ! oldChanges.containsKey(newBibId) )
        continue;
      List<Change> changesToRemove = new ArrayList<>();
      for ( Change c : newChanges.get(newBibId) )
        if (oldChanges.get(newBibId).contains(c))
          changesToRemove.add(c);
      newChanges.get(newBibId).removeAll(changesToRemove);
      if (newChanges.get(newBibId).isEmpty())
        bibsToRemove.add(newBibId);
    }
    System.out.println("bibs eliminated from newChanges: "+bibsToRemove.toString());
    for (Integer i : bibsToRemove)
      newChanges.remove(i);
    System.out.println("No, really. See?: "+newChanges.toString());
    return newChanges;
  }

  private static SolrInputDocument xml2SolrInputDocument(String xml) throws XMLStreamException {
    SolrInputDocument doc = new SolrInputDocument();
    XMLInputFactory input_factory = XMLInputFactory.newInstance();
    XMLStreamReader r  = 
        input_factory.createXMLStreamReader(new StringReader(xml));
    while (r.hasNext()) {
      if (r.next() == XMLStreamConstants.START_ELEMENT) {
        if (r.getLocalName().equals("doc")) {
          for (int i = 0; i < r.getAttributeCount(); i++)
            if (r.getAttributeLocalName(i).equals("boost"))
              doc.setDocumentBoost(Float.valueOf(r.getAttributeValue(i)));
        } else if (r.getLocalName().equals("field")) {
          String fieldName = null;
          Float boost = null;
          for (int i = 0; i < r.getAttributeCount(); i++)
            if (r.getAttributeLocalName(i).equals("name"))
              fieldName = r.getAttributeValue(i);
            else if (r.getAttributeLocalName(i).equals("boost"))
              boost = Float.valueOf(r.getAttributeValue(i));
          if (boost != null)
            doc.addField(fieldName, r.getElementText(), boost);
          else
            doc.addField(fieldName, r.getElementText());
        }
      }
    }
    return doc;
  }

}