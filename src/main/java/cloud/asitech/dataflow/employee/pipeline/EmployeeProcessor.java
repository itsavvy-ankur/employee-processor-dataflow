package cloud.asitech.dataflow.employee.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import cloud.asitech.dataflow.employee.utils.WriteOneFilePerWindow;

public class EmployeeProcessor {

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
    }

    static void runEmployeeProcessor(EmployeePipelineOptions options) {
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform,
        // and passes the
        // static FormatAsTextFn() to the ParDo transform.

        p.apply("ReadLines",
                PubsubIO.readStrings().fromSubscription(options.getInputTopic()))
                .apply("To Upper Case", ParDo.of(new EmployeeUpperCaseDoFn()))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Write Files to GCS", new WriteOneFilePerWindow(options.getOutput(), 1));
                       
                  /*      .apply("WriteCounts", TextIO.write().withWindowedWrites()
                        .withNumShards(1).to(options.getOutput())); */

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
