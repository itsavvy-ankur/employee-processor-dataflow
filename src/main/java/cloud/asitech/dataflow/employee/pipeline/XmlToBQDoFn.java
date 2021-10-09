package cloud.asitech.dataflow.employee.pipeline;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import cloud.asitech.dataflow.employee.domain.Employee;

public class XmlToBQDoFn extends PTransform<PCollection<String>, PCollection<TableRow>> {
    XmlMapper xmlMapper = new XmlMapper();
    ObjectMapper mapper = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public PCollection<TableRow> expand(PCollection<String> input) {

        return input.apply("XmlToTableRow", MapElements.<String, TableRow>via(new SimpleFunction<String, TableRow>() {
            @Override
            public TableRow apply(String input) {
                try {
                    Employee e = xmlMapper.readValue(input, Employee.class);
                    byte[] json = mapper.writeValueAsBytes(e);
                    TableRow tr = TableRowJsonCoder.of().decode(new ByteArrayInputStream(json), Coder.Context.OUTER);
                    tr.set("bq_ins_dt", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    return tr;
                } catch (IOException e1) {

                    throw new RuntimeException("unable to parse input", e1);
                }
            }

        }));
    };

}
