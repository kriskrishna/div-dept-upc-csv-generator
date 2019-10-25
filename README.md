Div-Dept-Upc-Csv-Generator
Dataflow batch job that reads records from Catalog table  and generates CSV file(s).

## Input Source
Striim team loads the catalog data in to `ProdSku` Spanner table. The batch job reads the SkuUPC records using SpannerIO source. Entire table is read and only the following columns are read 
 - `LocNbr` (Division Number)
 - `DeptNbr` (Department Number)
 - `SkuUpcNbr` (Sku Upc Number)

## Transformations
As part of the internal transformation Struct records from above Spanner table are converted to KV<String, String> with key as `"LocNbr_DeptNbr"` and value as `"SkuUpcNbr"`. The KV records are grouped by `"LocNbr_DeptNbr"` before they are sent to sink to save as CSV files.

## Sink
FileIO dynamic write function is used to save the grouped records into different files. The files are grouped by the key `"LocNbr_DeptNbr"` while the file name is formatted as `"LocNbr_DeptNbr.csv"`. The content of the file starts with `Div#, Dept#` followed by collection of `SkuUpc#` separated by `,`. Different CSV files are created for every unqiue combination of Div# and Dept#.

### Creating Job Template
Jenkins pipeline is expected to run the following command to generate template json and push it to cloud
```sh
mvn compile exec:java -Dexec.mainClass=com.macys.common.pricing.services.upccsvgenerator.batch.job.DivDeptUpcCsvGeneratorJob -Dexec.args="--runner=DataflowRunner --project=mtech-commonsvc-pricing-poc --stagingLocation=gs://csp-dataflow-dev/upccsvgenerator/staging --subnetwork=regions/us-central1/subnetworks/central1 --usePublicIps=false --streaming=false --appName=upccsvgeneratorjob --templateLocation=gs://csp-dataflow-dev/upccsvgenerator/templates/UpcCsvGenerator.json --tempLocation=gs://csp-dataflow-dev/upccsvgenerator/tmp "
```

### Running Batch Job
Batch job is scheduled to trigger daily and expected to complete before the [UPC Cascade batch job](https://code.devops.fds.com/commonservices/pricing/pricingchangebatchsynch) starts. Cloud scheduler triggers the job at a configured scheduled time.
Ex: 

URL to trigger: 
```json
https://dataflow.googleapis.com/v1b3/projects/mtech-commonsvc-pricing-poc/templates:launch?gcsPath=gs%3A%2F%2Fcsp-dataflow-dev%2Fupccsvgenerator%2Ftemplates%2FUpcCsvGenerator.json
```
```html
Http Method: POST
```
Body Json 
```json 
{
   "parameters":{
        "spannerDatabaseId": "dev",
        "spannerInstanceId": "price-upm",
        "spannerProjectId": "mtech-commonsvc-pricing-poc",
        "upcDestinationFilePath": "gs://csp-dataflow-dev/locnupcpricecascade/input"
   },
   "environment":{
      "numWorkers":2,
      "maxWorkers":100
   }
}
```