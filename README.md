# Employee Processor Dataflow Job

## Dataaflow Runner instructions

```shell
mvn compile exec:java -Dexec.mainClass=cloud.asitech.dataflow.employee.pipeline.EmployeeProcessor \
     -Dexec.args="--runner=DataflowRunner --project=<update_me> \
                  --region=us-east1 \
                  --serviceAccount=dataflow-processor-sa@<update_me>.iam.gserviceaccount.com \
                  --gcpTempLocation=gs://<update_me>/tmp \
                  --inputTopic=projects/<update_me>/topics/big2gcp-img --output=gs://<update_me>/employee" \
     -Pdataflow-runner
```