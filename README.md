# Dataproc on Google Cloud Platform 

<sub/>

In this scenario the task is to match transaction on debit cards with transations on current accounts. We do not have any immediate key or indicator, that we could use.
<br/> 

For this purpose we have to use account number, total amount of transaction and location for specific transaction. 


<p align="center">

<img width="800" alt="Zrzut ekranu 2023-09-23 o 17 32 12" src="https://github.com/eda6767/dataproc/assets/102791467/6e444932-57b9-42ce-9709-27fd95478f95">
</p>


<p align="center">

<img width="600" alt="Zrzut ekranu 2023-09-23 o 17 43 11" src="https://github.com/eda6767/dataproc/assets/102791467/c8c6b9fd-4261-4892-9fd7-ffd9870ed8e1">


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


