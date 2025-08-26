# Project Description

*da-solr-availability* was created to replace the holdings and availability logic imported
from [cornell-voyager-backend](https://github.com/cul-it/cornell-voyager-backend).
Cornell-voyager-backend pulled availability on demand to display in the Cornell OPAC,
sometimes with significant delays in the public view. *da-solr-availability* applies the
same logic, but continuously, as availability and holdings change to be fed into the
Solr index that serves the OPAC. That way, the information will already be retrieved,
cached, and pre-summarized to be easily displayed on the public interface without a
patron-visible delay.

In addition to supplying current availability, this project also provides summaries of
holding and item information for convenient and quick display.

Jira issue (not public):
 [DISCOVERYACCESS-3550](https://culibrary.atlassian.net/browse/DISCOVERYACCESS-3550)

