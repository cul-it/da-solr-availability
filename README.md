
*da-solr-availability* should include the holdings and availability logic imported from
[cornell-voyager-backend](https://github.com/cul-it/cornell-voyager-backend).
Cornell-voyager-backend pulls availability on demand to display in the Cornell OPAC,
sometimes with significant delays in the public view. *da-solr-availability* applies the
same logic, but continuously, as availability and
holdings change to be fed into the
Solr index that serves the OPAC. That way, the information will already be retrieved,
cached, and pre-summarized to be easily displayed on the public interface without a
patron-visible delay.

In addition to supplying current availability, this project will import the logic creating
JSON summaries of holding and item records from
[HoldingsAndItems.java](https://github.com/cul-it/da-solr/blob/20f93e3661a739833b78b184add305c781ef0c30/src/main/java/edu/cornell/library/integration/indexer/solrFieldGen/HoldingsAndItems.java)
in the [da-solr](https://culibrary.atlassian.net/browse/DISCOVERYACCESS-3550) project.
This last is in-progress, as the original HoldingsAndItems.java is still being depended on for a subset of its logic.

Jira issue (not public):
 [DISCOVERYACCESS-3550](https://culibrary.atlassian.net/browse/DISCOVERYACCESS-3550)