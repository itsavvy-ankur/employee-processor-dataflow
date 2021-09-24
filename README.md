# Employee Processor Dataflow Job

## Dataaflow Runner instructions

```shell
mvn compile exec:java -Dexec.mainClass=cloud.asitech.dataflow.employee.pipeline.EmployeeProcessor \
     -Dexec.args="--runner=DataflowRunner --project=<Update> \
                  --region=us-east1 \
                  --gcpTempLocation=gs://<Update>/tmp \
                  --inputTopic=projects/<Update>/subscriptions/big2gcp-img-sub --output=gs://<Update>/employee" \
     -Pdataflow-runner
```