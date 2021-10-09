package cloud.asitech.dataflow.employee.pipeline;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import cloud.asitech.dataflow.employee.domain.Employee;

import com.google.api.client.util.DateTime;

public class XmlToBQDoFn extends PTransform<PCollection<String>, PCollection<TableRow>> {
    XmlMapper xmlMapper = new XmlMapper();
    ObjectMapper mapper = new ObjectMapper();

    public PCollection<TableRow> expand(PCollection<String> input) {

        return input.apply("XmlToTableRow", MapElements.<String, TableRow>via(new SimpleFunction<String, TableRow>() {
            @Override
            public TableRow apply(String input) {
                try {
                    Employee e = xmlMapper.readValue(input, Employee.class);
                    byte[] json = mapper.writeValueAsBytes(e);
                    TableRow tr = TableRowJsonCoder.of().decode(new ByteArrayInputStream(json));
                    tr.set("bq_ins_dt", new DateTime(new Date()));
                    return tr;
                } catch (IOException e1) {

                    throw new RuntimeException("unable to parse input", e1);
                }
            }

        }));
    };

}
