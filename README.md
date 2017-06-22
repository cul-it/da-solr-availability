**This is project is just beginning, and is not yet in production.**

The plan for *da-solr-availability* is to import the holdings and availability logic from
[cornell-voyager-backend](https://github.com/cul-it/cornell-voyager-backend).
Cornell-voyager-backend pulls availability on demand to display in the Cornell OPAC,
sometimes with significant delays in the public view. The intention is that
*da-solr-availability* will apply the same logic, but continuously, as availability and
holdings change to feed into an availability database cache that will be fed into the
Solr index that serves the OPAC. That way, the information will already be retrieved,
cached, and pre-summarized to be easily displayed on the public interface without a
patron-visible delay.

In addition to supplying current availability, this project will import the logic creating
JSON summaries of holding and item records from
[HoldigsAndItems.java](https://github.com/cul-it/da-solr/blob/20f93e3661a739833b78b184add305c781ef0c30/src/main/java/edu/cornell/library/integration/indexer/solrFieldGen/HoldingsAndItems.java)
in the [da-solr](https://culibrary.atlassian.net/browse/DISCOVERYACCESS-3550) project.

Jira issue (not public):
 [DISCOVERYACCESS-3550](https://culibrary.atlassian.net/browse/DISCOVERYACCESS-3550)