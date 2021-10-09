package cloud.asitech.dataflow.employee.pipeline;

import com.google.api.services.bigquery.model.TimePartitioning;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import cloud.asitech.dataflow.employee.utils.WriteOneFilePerWindow;

public class EmployeeProcessor {

    private static final int BATCH_INTERVAL = 1;
    private static final int NUM_SHARDS = 100;
    private static final String TIME_PARTITIONING_COLUMN = "bq_ins_dt";

    public interface EmployeePipelineOptions extends StreamingOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Input pub/sub topic name")
        @Required
        String getInputTopic();

        void setInputTopic(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);

        @Description("BQ Table detail via the TableReference Scheme")
        @Required
        String getTableDetail();

        void setTableDetail(String value);

    }

    static void runEmployeeProcessor(EmployeePipelineOptions options) {
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform,
        // and passes the
        // static FormatAsTextFn() to the ParDo transform.

        PCollection<String> messages = p.apply("ReadLines",
                PubsubIO.readStrings().fromSubscription(options.getInputTopic()));

                
        messages.apply("convToTR", new XmlToBQDoFn()).apply("WriteToBQ",
                BigQueryIO.writeTableRows().withTriggeringFrequency(Duration.standardMinutes(BATCH_INTERVAL))
                        .withMethod(Method.FILE_LOADS).withNumFileShards(NUM_SHARDS)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withTimePartitioning(new TimePartitioning().setField(TIME_PARTITIONING_COLUMN))
                        .to(BigQueryHelpers.parseTableSpec(options.getTableDetail())));

        messages.apply("To Upper Case", ParDo.of(new EmployeeUpperCaseDoFn()))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Write Files to GCS", new WriteOneFilePerWindow(options.getOutput(), 1));

        p.run().waitUntilFinish();
    }

    public static class EmployeeUpperCaseDoFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String employee, OutputReceiver<String> out) {
            // Use OutputReceiver.output to emit the output element.
            out.output(employee.toUpperCase());
        }

    }

    public static void main(String[] args) {
        EmployeePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(EmployeePipelineOptions.class);

        runEmployeeProcessor(options);
    }

}
