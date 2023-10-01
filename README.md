# Dataproc on Google Cloud Platform 

<sub/>


<br>
In this scenario the task is to match transaction on debit cards with transations on current accounts. We do not have any primary key or indicator, that we could use for matching.
<br/> 
<br>
For this purpose we have to use account number, total amount of transaction and location for specific transaction. 
<br/> 

<p align="center">

<img width="600" alt="Zrzut ekranu 2023-09-23 o 17 32 12" src="https://github.com/eda6767/dataproc/assets/102791467/6e444932-57b9-42ce-9709-27fd95478f95">
</p>



<p align="center">

<img width="600" alt="Zrzut ekranu 2023-09-23 o 17 48 13" src="https://github.com/eda6767/dataproc/assets/102791467/c8561190-6216-423a-afe2-a9b5a049e1a8">

</p>


<br/> 
From every data source we are gonna read data from BigQuery table using following method:
<br/> 


```
    query = "SELECT * FROM {table} where DUE_DT = {date}".format(table=table_name, date = date_in)

    crnt_acct_trx_fcd = spark.read.format("bigquery")\
        .option("query", query)\
        .load()

    crnt_acct_trx_fcd.show()
```


<br/> 
At each stage of correct translation matching, the result will be saved to the target table according to the following method
<br/> 

```
 result_1.write.format('bigquery') \
        .mode("append")\
        .option('table', dpcrnt_acct_trx_fcd) \
        .save()
```

<br/> 
</br>
<img width="500" alt="Zrzut ekranu 2023-10-1 o 12 37 08" src="https://github.com/eda6767/dataproc/assets/102791467/2a1f83e3-6e6a-4824-a8dc-9c68d7e6d43e">


<br/> 
</br>
<img width="600" alt="Zrzut ekranu 2023-10-1 o 12 39 48" src="https://github.com/eda6767/dataproc/assets/102791467/6a060c63-8cb5-4d8b-b16e-636638976757">

<br/> 
</br>

<img width="1255" alt="Zrzut ekranu 2023-10-1 o 12 44 20" src="https://github.com/eda6767/dataproc/assets/102791467/5336e286-5484-42bb-b3bb-f90078099c26">


<br/> 
</br>

```
export REGION=europe-west1
gcloud dataproc autoscaling-policies import dataproc_autoscaling --source=policy.yaml --region=${REGION}
```

<br/> 
</br>

```
export PROJECT='clean-sylph-377411'
export CLUSTER_NAME=dataproc-demo2 
export REGION=europe-west1

gcloud dataproc clusters create ${CLUSTER_NAME} --region ${REGION} --zone europe-west1-b  --project ${PROJECT}  --master-machine-type n1-standard-2 --master-boot-disk-size 500 --image-version 2.0-debian10  --num-workers 2 --worker-machine-type n2-standard-4 --worker-boot-disk-size 500 --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh --metadata bigquery-connector-version=1.2.0 --metadata spark-bigquery-connector-version=0.21.0

```

<br/> 
</br>

```
gcloud dataproc jobs submit spark --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery_2.11:0.9.1-beta
```

<br/> 
</br>



```
gcloud dataproc clusters stop dataproc-demo2 --region ${REGION}
gcloud dataproc clusters delete dataproc-demo2 --region ${REGION}
```
