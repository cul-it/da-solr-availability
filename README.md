# Project Description

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

# Running Project Code

To run project code, three jars included in the repo need to be installed to the local
Maven store. This is logic should be moved to the `pom.xml`, but in the meantime executing
these commands from the main project directory is an effective workaround.

```
mvn install:install-file -DgroupId=com.oracle \
  -DartifactId=ojdbc7 -Dversion=12.1.0.2 \
  -Dpackaging=jar -Dfile=lib/ojdbc7-12.1.0.2.jar \
  -DgeneratePom=true

mvn install:install-file -DgroupId=edu.cornell.library.integration \
  -DartifactId=da-solr-marc -Dversion=1 \
  -Dpackaging=jar -Dfile=lib/da-solr-marc.jar \
  -DgeneratePom=true

mvn install:install-file -DgroupId=org.marc4j \
  -DartifactId=marc4j -Dversion=2.6.0 \
  -Dpackaging=jar -Dfile=lib/marc4j-2.6.0.jar \
  -DgeneratePom=true
```