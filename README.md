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



