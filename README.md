# Dataproc on Google Cloud Platform 



<br>
<br/> 

<sub>
In this scenario the task is to match transaction on debit cards with transations on current accounts. We do not have any primary key or indicator, that we could use for matching.
For this purpose we have to use account number, total amount of transaction and location for specific transaction. </sub>


<p align="center"> 
<img width="450" alt="Zrzut ekranu 2023-10-16 o 19 18 45" src="https://github.com/eda6767/dataproc/assets/102791467/b5c6afce-5e8e-44e2-bc7b-3be57b7e7583">
</p>

<p align="center">

<img width="450" alt="Zrzut ekranu 2023-10-16 o 19 38 15" src="https://github.com/eda6767/dataproc/assets/102791467/3952bb0a-3cf0-455b-92fa-53eea620df96">
</p>

<sub> Here we are gonna focuse more on technical aspects and different way how to launch dataproc job. As a first step we have to define parameters like _temporaryGcsBucket, viewsEnabled and materializationDataset_ 
</sub>

<sub> _temporaryGcsBucket_  - this is a parameter required for writing data to BigQuery. Spark connector to BigQuery contains two write options - direct and indirect method while writing data into BigQuery. This is an optional parameter while indirect method is default method for writing data. So, if you are using indirect method - this requires defininf temporary GCS Bucket. _The connector writes the data to BigQuery by first buffering all the data into a Cloud Storage temporary table. Then it copies all data from into BigQuery in one operation._ Another options is direct method - there data is written directly to BigQuery using BigQuery Storage Write API. </sub>
<br>
<br/> 
<sub> _viewsEnabled_ - this parameter enable to read from not only table, but also from views  </sub> 
<br>
<br/> 
<sub>  _materializationDataset_  - The dataset where the materialized view is going to be created. This dataset should be in same location as the view or the queried tables. </sub> 

<sub>
    
```
bucket="dataproc_mcc_proc"
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "dataproc")
```
</sub>

<sub> From every data source we are gonna read data from BigQuery table using following method: </sub>



<sub>

```
    query = "SELECT * FROM {table} where DUE_DT = {date}".format(table=table_name, date = date_in)

    crnt_acct_trx_fcd = spark.read.format("bigquery")\
        .option("query", query)\
        .load()

    crnt_acct_trx_fcd.show()
```

</sub>


<sub>
At each stage of correct translation matching, the result will be saved to the target table according to the following method </sub>

<sub>

```
 result_1.write.format('bigquery') \
        .mode("append")\
        .option('table', dpcrnt_acct_trx_fcd) \
        .save()
```

</sub>


<sub> To use Dataproc cluster, first we have to enable Dataproc API, just by finding proper service and clicking Enable button, or using gcloud command as below </sub>

<sub>
    
```
gcloud config set project YOUR_PROJECT_ID
gcloud services enable SERVICE_NAME
gcloud services list --available
```

</sub>
<br/> 
</br>
<img width="500" alt="Zrzut ekranu 2023-10-1 o 12 37 08" src="https://github.com/eda6767/dataproc/assets/102791467/2a1f83e3-6e6a-4824-a8dc-9c68d7e6d43e">
<br/> 
</br>

<sub> As as first method we are gonna create Dataproc manually and start Dataproc job using Google Cloud Console. For testing processes with not huge amount of data we could use Single Node option, but for out purpose, when we have quite big amount of data, let's use first option with 2 workers and 1 stanard node. </sub>

<br/> 
</br>

<img width="600" alt="Zrzut ekranu 2023-10-1 o 12 39 48" src="https://github.com/eda6767/dataproc/assets/102791467/6a060c63-8cb5-4d8b-b16e-636638976757">


<br/> 
</br>

<sub> Alternatively we can use gcloud command  with the appropriate properties. Please note that in our case we needed to define an initialization action consisting in installing the connector and, what is very important, defining metadata specifying the version of the connector for bigquery. </sub>


<sub>
    
```
export PROJECT='clean-sylph-377411'
export CLUSTER_NAME=dataproc-demo2 
export REGION=europe-west1

gcloud dataproc clusters create ${CLUSTER_NAME} --region ${REGION} --zone europe-west1-b  --project ${PROJECT}  --master-machine-type n1-standard-2 --master-boot-disk-size 500 --image-version 2.0-debian10  --num-workers 2 --worker-machine-type n2-standard-4 --worker-boot-disk-size 500 --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh --metadata bigquery-connector-version=1.2.0 --metadata spark-bigquery-connector-version=0.21.0

```
 </sub>

<br/> 
</br>


<sub> Additionally, if we want to have control over the autoscaling of nodes, we can create the so-called autoscaling policy in the yaml file </sub>

<img width="1255" alt="Zrzut ekranu 2023-10-1 o 12 44 20" src="https://github.com/eda6767/dataproc/assets/102791467/5336e286-5484-42bb-b3bb-f90078099c26">
<br/> 
</br>
<sub>
    
```
export REGION=europe-west1
gcloud dataproc autoscaling-policies import dataproc_autoscaling --source=policy.yaml --region=${REGION}
```
</sub>
<br/> 
</br>

<sub> If we want to stop of delete cluster, we can use commands as below </sub>


<sub>

```
gcloud dataproc clusters stop dataproc-demo2 --region ${REGION}
gcloud dataproc clusters delete dataproc-demo2 --region ${REGION}
```
</sub>

<br/> 
</br>

<sub> Now, when we have cluster ready, we are able to create job. This is an example how to do it using Google Console. We have to define job name, created previously cluster, complete the main PySpark file location in Cloud Bucket. </sub>


<img width="600" alt="Zrzut ekranu 2023-10-22 o 13 27 16" src="https://github.com/eda6767/dataproc/assets/102791467/2a6fc2dc-31da-4e22-acce-44c083fb9241">

<img width="600" alt="Zrzut ekranu 2023-10-22 o 13 26 17" src="https://github.com/eda6767/dataproc/assets/102791467/10e9cabb-f1ee-427c-80da-1f851febc2a8">


<sub>
    
```
gcloud dataproc jobs wait job-54195ea1 --project balmy-geography-374018 --region europe-west1
```
</sub>

<br/> 
</br>

<sub>
    
```
gcloud dataproc workflow-templates instantiate template-7ac4f --region=europe-west1
```
</sub>
