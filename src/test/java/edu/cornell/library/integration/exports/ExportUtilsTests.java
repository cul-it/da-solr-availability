package edu.cornell.library.integration.exports;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Test;

import edu.cornell.library.integration.exports.ExportUtils.FieldRange;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class ExportUtilsTests {

  @Test
  public void cleanUnwantedDataFieldsTest() {
    MarcRecord r = beforeMarc();

    // confirm before
    assertEquals("041 100 111 404 444 505 555 ITM 734 777 888 999 734 500 888 NEW",
        String.join(" ", r.dataFields.stream().map(f -> f.tag).collect(Collectors.toList())));

    // remove all instances of field tag
    ExportUtils.cleanUnwantedDataFields(r, Arrays.asList("734"), null, false);
    assertEquals("041 100 111 404 444 505 555 ITM 777 888 999 500 888 NEW",
        String.join(" ", r.dataFields.stream().map(f -> f.tag).collect(Collectors.toList())));

    // remove tag range by value not field order. Range is inclusive
    r = beforeMarc();
    ExportUtils.cleanUnwantedDataFields(r, null, Arrays.asList(new FieldRange("400", "505")), false);
    assertEquals("041 100 111 555 ITM 734 777 888 999 734 888 NEW",
        String.join(" ", r.dataFields.stream().map(f -> f.tag).collect(Collectors.toList())));

    // remove non numeric fields by rule
    r = beforeMarc();
    ExportUtils.cleanUnwantedDataFields(r, null, null, true);
    assertEquals("041 100 111 404 444 505 555 734 777 888 999 734 500 888",
        String.join(" ", r.dataFields.stream().map(f -> f.tag).collect(Collectors.toList())));

    // remove non numeric fields by name
    r = beforeMarc();
    ExportUtils.cleanUnwantedDataFields(r, Arrays.asList("734", "ITM", "777"), null, false);
    assertEquals("041 100 111 404 444 505 555 888 999 500 888 NEW",
        String.join(" ", r.dataFields.stream().map(f -> f.tag).collect(Collectors.toList())));

    // all rules applied
    r = beforeMarc();
    ExportUtils.cleanUnwantedDataFields(r, Arrays.asList("734","777"), Arrays.asList(new FieldRange("400","505")), true);
    assertEquals("041 100 111 555 888 999 888",
        String.join(" ", r.dataFields.stream().map(f -> f.tag).collect(Collectors.toList())));
  }
  private MarcRecord beforeMarc() {
    MarcRecord r = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
    r.dataFields.addAll(Arrays.asList(
        new DataField(1,"041"), new DataField(2,"100"), new DataField(3,"111"), new DataField(4,"404"),
        new DataField(5,"444"), new DataField(6,"505"), new DataField(7,"555"), new DataField(8,"ITM"),
        new DataField(9,"734"), new DataField(10,"777"),new DataField(11,"888"),new DataField(12,"999"),
        new DataField(13,"734"),new DataField(14,"500"),new DataField(15,"888"),new DataField(16,"NEW")
        ));
    return r;
  }
}
