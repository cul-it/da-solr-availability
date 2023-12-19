
# LocationFacetMapping.xslx

The Excel spreadsheet should be where editing of the location facet mapping takes place,
but the data needs to be exported into a tab delimited file that will power the live processing.

# LocationFacetMapping.txt

This should be tab-delimited UTF-8 data. With the spreadsheet open in Excel, choose
File->Save As, then choose "Unicode Text (*.txt)". Save to a temportary filename (e.g. tmp.txt).
This will write the file out in a tab delimited format using UTF-16. (The more obvious format
option of "Text (Tab delimited) (*.txt)" is not unicode at all.) While we could work against
the UTF-16 data, UTF-8 is diffable by GitHub, making it a more friendly format for tracking and
understanding changes. This requires an extra step to convert the file:

```
iconv -f UTF-16 -t utf8 tmp.txt > LocationFacetMapping.txt
```