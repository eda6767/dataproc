#!/usr/bin/python

from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import sum, avg, max



def main(date_in = "'2021-10-31'"):

    PROJECT="clean-sylph-377411"
    crnt_acct_trx_fcd= "{}.kpr.crnt_acct_trx_fcd".format(PROJECT)
    dbc_trx_fcd = "{}.kpr.dbc_trx_fcd".format(PROJECT)
    prd_hrchy_dim = "{}.kpr.prd_hrchy_dim".format(PROJECT)
    depo_trx_type_dct= "{}.kpr.depo_trx_type_dct".format(PROJECT)
    dpcrnt_acct_trx_fcd = "{}.agr.dpcrnt_acct_trx_fcd".format(PROJECT)

    print("Number of arguments: {0} arguments.".format(len(sys.argv)))
    print("Argument List: {0}".format(str(sys.argv)))


    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName('dataproc-python-demo') \
        .getOrCreate()


    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    bucket="dataproc_mcc_proc"
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "dataproc")

    query = "SELECT * FROM {table} where DUE_DT = {date}".format(table=crnt_acct_trx_fcd, date = date_in)

    crnt_acct_trx_fcd = spark.read.format("bigquery")\
        .option("query", query)\
        .load()

    crnt_acct_trx_fcd.show()

    query2 = "SELECT * FROM {table} where {date} between START_DT and END_DT".format(table=prd_hrchy_dim, date=date_in)

    prd_hrchy_dim = spark.read.format("bigquery") \
        .option('dataset', 'kpr') \
        .option("query", query2) \
        .load()

    prd_hrchy_dim.show()

    query3 = "SELECT * FROM {table} where DUE_DT={date}".format(table=dbc_trx_fcd, date=date_in)

    dbc_trx_fcd = spark.read.format("bigquery") \
        .option('dataset', 'kpr') \
        .option("query", query3) \
        .load()

    dbc_trx_fcd.show()

    logging.info('Read BigQuery - depo_trx_type_dct')
    query4 = "SELECT * FROM {table} where {date} between START_DT and END_DT".format(table=depo_trx_type_dct, date=date_in)

    depo_trx_type_dct = spark.read.format("bigquery") \
        .option('dataset', 'kpr') \
        .option("query", query4) \
        .load()

    depo_trx_type_dct.show()
    logging.info('Read BigQuery - depo_trx_type_dct - finished')

    a = crnt_acct_trx_fcd.alias("a")
    b = dbc_trx_fcd.alias("b")
    c = depo_trx_type_dct.alias("c")
    d = prd_hrchy_dim.alias("d")

    df1 = a.join(prd_hrchy_dim, ['prd_key', 'sys_cd'], 'left').join(c, ['trx_cd', 'sys_cd'], 'left')

    df2_mcc = df1.filter(df1['cat_data_prep_cd'] == 'MCC')
    df2_mcc = df2_mcc.select('acc_dt', 'acct_cur_cd', 'acct_id', 'acct_key', 'acc_type_num', 'bal_amt_cur',
                             'bal_amt_pln'
                             , 'bank_id', 'chnl_cd', 'contract_id', 'crnt_acct_trx_key', 'cur_cd', 'due_dt',
                             'frst_acct_id'
                             , 'frst_acct_key', 'kir_branch_id', 'main_acct_id', 'a.prd_id', 'prd_key', 'a.prd_type_cd'
                             , 'receiver_acct_id', 'receiver_acct_key', 'receiver_nm_address', 'receiver_orig_acct_id'
                             , 'sender_acct_id', 'sender_acct_key', 'sender_nm_address', 'sender_orig_acct_id',
                             'short_title'
                             , 'a.subprd_id', 'sys_cd', 'tech_acc_dt', 'a.tech_etl_pkg_cd', 'a.tech_insert_id',
                             'a.tech_insert_ts'
                             , 'title', 'trx_amt_cur', 'trx_amt_pln', 'trx_cd', 'trx_cur_rt', 'trx_dt', 'trx_num',
                             'trx_ts', 'value_dt')
    print('df2_mcc')
    df2_mcc.show()

    df2_prep = df2_mcc.select('acct_key', 'value_dt').dropDuplicates()

    logging.info('Join crnt_acct_trx_fcd [MCC] (distinct acct_key,value_key) with dbc_trx_fcd')

    print("columns for dbc_trx_fcd")
    print(dbc_trx_fcd.columns)

    print("columns for df2_prep")
    print(df2_prep.columns)


    df2 = dbc_trx_fcd.join(df2_prep, (df2_prep.acct_key == dbc_trx_fcd.acct_key) & (df2_prep.value_dt == dbc_trx_fcd.trx_dt),'inner').drop(df2_prep.acct_key)

    df2 = df2.select('acc_method_cd', 'acct_id', 'acct_key', 'acc_ts', 'acc_type_cd', 'bank_id', 'chnl_cd',
                     'contract_id', 'dbc_id', 'dbc_key', 'dbc_prd_id', 'dbc_prd_key', 'dbc_prd_type_cd', 'dbc_subprd_id',
                     'dbc_type_cd', 'due_dt', 'ext_acct_id', 'institution_cd', 'kir_branch_id', 'mcc_cd', 'multi_cur_flg', 'prd_id'
                     , 'prd_key', 'prd_type_cd', 'subprd_id', 'sys_cd', 'tech_etl_pkg_cd', 'tech_insert_id','tech_insert_ts'
                     , 'trx_address_nm', 'trx_amt_cur', 'trx_amt_pln', 'trx_auth_type_cd', 'trx_cat_cd', 'trx_cd'
                     , 'trx_city_nm', 'trx_country_cd', 'trx_cur_cd', 'trx_device_cd', 'trx_dt', 'trx_id', 'trx_key'
                     , 'trx_src_amt_cur', 'trx_src_amt_pln', 'trx_src_cur_cd', 'trx_stlmt_amt_cur', 'trx_stlmt_amt_pln'
                     , 'trx_stlmt_cur_cd', 'trx_stlmt_cur_rt', 'trx_ts', 'trx_type_cd')
    print('print df2')
    df2.show()

    df3 = df1.filter((~df1['cat_data_prep_cd'].isin(['MCC', 'PTH'])) | (
                (df1['prd_lvl4_cd'] != 'TECH') & (df1['cat_data_prep_cd'] == 'PTH')))

    print('input1')
    dpcrnt_acct_trx_fcd_input_1 = df3.select('a.crnt_acct_trx_key', 'due_dt', 'sys_cd', 'acct_key', 'bank_id',
                                             'kir_branch_id', 'contract_id', 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key', 'main_acct_id',
                                             'a.prd_key', 'a.prd_type_cd', 'a.prd_id', 'a.subprd_id', 'trx_ts',
                                             'trx_dt', 'acc_dt', 'chnl_cd', 'a.trx_cd', 'acc_type_num', 'short_title'
                                             , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt',
                                             'trx_amt_pln', 'trx_amt_cur', 'bal_amt_pln', 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id',
                                             'sender_orig_acct_id', 'sender_nm_address'
                                             , 'receiver_acct_key', 'receiver_acct_id', 'receiver_orig_acct_id','receiver_nm_address'
                                             , 'a.tech_etl_pkg_cd', 'a.tech_insert_id','a.tech_insert_ts', 'tech_acc_dt')
    dpcrnt_acct_trx_fcd_input_1.show()
    print(f"DataFrame Columns count : {dpcrnt_acct_trx_fcd_input_1.count()}")

    dpcrnt_acct_trx_fcd_input_1.write.format('bigquery') \
        .mode("append")\
        .option('table', dpcrnt_acct_trx_fcd) \
        .save()

    print("----- Saving results to destination tables - finished")

    print('Preparation for grouping')
    group_prep = df2.withColumn('regex_address',
                                regexp_replace(df2.trx_address_nm, r"([\-\*\#\$\ \,\.\%\[\]\'\"])", ''))
    group_prep = group_prep.withColumn('coalesce_trx', F.round(
        abs(coalesce(df2['trx_stlmt_amt_pln'], df2['trx_stlmt_amt_pln'], df2['trx_src_amt_pln'], lit(0)))))
    group_prep.show()

    print('First grouping')
    logging.info('First grouping - dbc_trx_fcd')
    group_cols = ['acct_key', 'trx_dt', 'coalesce_trx', 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm',
                  'chnl_cd']
    df4 = group_prep.groupBy(group_cols).agg(max('trx_auth_type_cd').alias('max_trx_auth_type_cd'), \
                                             max('trx_device_cd').alias('max_trx_device_cd'), \
                                             max('trx_address_nm').alias('max_trx_address_nm'), \
                                             max('regex_address').alias('max_regex_address'))
    df4 = df4.withColumnRenamed('trx_dt', 'dbc_trx_dt').withColumnRenamed('chnl_cd', 'dbc_chnl_cd')
    df4.show()

    a4 = df4.alias("a4")

    print('Second grouping')
    logging.info('Second grouping - dbc_trx_fcd')
    df5 = group_prep.groupBy(
        ['acct_key', 'trx_dt', 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm', 'chnl_cd']).agg( \
        sum('coalesce_trx').alias('sum_coalesce_trx'), \
        max('trx_auth_type_cd').alias('max_trx_auth_type_cd'), \
        max('trx_device_cd').alias('max_trx_device_cd'), \
        max('trx_address_nm').alias('max_trx_address_nm'), \
        max('regex_address').alias('max_regex_address')).select('acct_key', 'trx_dt', 'mcc_cd', 'dbc_key', 'dbc_id',
                                                                'trx_country_cd', 'trx_city_nm', 'chnl_cd',
                                                                'sum_coalesce_trx', 'max_trx_auth_type_cd',
                                                                'max_trx_device_cd', 'max_trx_address_nm',
                                                                'max_regex_address')
    df5 = df5.withColumnRenamed('trx_dt', 'dbc_trx_dt').withColumnRenamed('chnl_cd', 'dbc_chnl_cd')

    df5.show()

    # 6_1
    logging.info('Preparing (distinct acct_key,trx_dt)')
    df6_prep = df2.select('acct_key', 'trx_dt').dropDuplicates()

    logging.info('Joining dbc_acct_trx_fcd (distinct acct_key, trx_dt) to crnt_acct_trx_fcd')
    mcc = df2_mcc.alias("mcc")
    prep = df6_prep.alias("prep")

    df6 = mcc.join(prep, (mcc.acct_key == prep.acct_key) & (prep.trx_dt == mcc.value_dt), 'left')
    df6 = df6.select('acc_dt', 'acct_cur_cd', 'acct_id', 'mcc.acct_key', 'acc_type_num', 'bal_amt_cur', 'bal_amt_pln'
                     , 'bank_id', 'chnl_cd', 'contract_id', 'crnt_acct_trx_key', 'cur_cd', 'due_dt', 'frst_acct_id'
                     , 'frst_acct_key', 'kir_branch_id', 'main_acct_id', 'prd_id', 'prd_key', 'prd_type_cd'
                     , 'receiver_acct_id', 'receiver_acct_key', 'receiver_nm_address', 'receiver_orig_acct_id'
                     , 'sender_acct_id', 'sender_acct_key', 'sender_nm_address', 'sender_orig_acct_id', 'short_title'
                     , 'subprd_id', 'sys_cd', 'tech_acc_dt', 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts'
                     , 'title', 'trx_amt_cur', 'trx_amt_pln', 'trx_cd', 'trx_cur_rt', 'mcc.trx_dt', 'trx_num', 'trx_ts',
                     'value_dt')

    df6 = df6.withColumn('regex_title', regexp_replace(df6.title, r"([\-\*\#\$\ \,\.\%\[\]\'\"])", ''))
    df6 = df6.withColumn('mod_trx_amt_pln', F.round(abs('trx_amt_pln')))
    df6 = df6.withColumn('dopisz_mcc', when(df6.acct_key.isNotNull() & df6.trx_dt.isNotNull(), "1").otherwise("0"))
    df6.show()

    df6_mcc = df6.filter(df6.dopisz_mcc == '1')
    df6_no_mcc = df6.filter(df6.dopisz_mcc == '0').select('crnt_acct_trx_key')


    logging.info('Selecting transactions for first attempt of assigning MCC')

    logging.info('List of crnt_acct_trx_key - first attempt')
    df7 = df6_mcc.select('crnt_acct_trx_key')
    df7.show()

    df6_mcc = df6_mcc.withColumn('new_title', upper(trim(df6_mcc.title)))
    df6_mcc.show()
    a4 = a4.withColumn('new_trx_city_nm', trim(a4.trx_city_nm))
    a4.show()

    a6mcc = df6_mcc.alias('a6mcc')

    logging.info('Joining First Grouping with crnt_acct_trxs with <remove like> ')
    df7_1 = a6mcc.join(a4, (a4.acct_key == a6mcc.acct_key) & (a4.dbc_trx_dt == a6mcc.value_dt)
                       & (a4.coalesce_trx == a6mcc.mod_trx_amt_pln) & F.expr("new_title like new_trx_city_nm") & F.expr(
        "max_regex_address like regex_title"), 'left')
    df7_1.show()

    df7_2 = df7.join(df7_1, ['crnt_acct_trx_key'], 'left')
    df7_2 = df7_2.select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'a6mcc.acct_key', 'bank_id', 'kir_branch_id',
                         'contract_id'
                         , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key', 'main_acct_id', 'prd_key',
                         'prd_type_cd'
                         , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt', 'a6mcc.chnl_cd', 'trx_cd',
                         'acc_type_num', 'short_title'
                         , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt', 'trx_amt_pln', 'trx_amt_cur',
                         'bal_amt_pln'
                         , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id', 'sender_orig_acct_id',
                         'sender_nm_address'
                         , 'receiver_acct_key', 'receiver_acct_id', 'receiver_orig_acct_id', 'receiver_nm_address'
                         , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts', 'tech_acc_dt'
                         , 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm',
                         col('max_trx_auth_type_cd').alias('trx_auth_type_cd'),
                         col('max_trx_address_nm').alias('trx_address_nm'),
                         col('max_trx_device_cd').alias('trx_device_cd'), 'dbc_chnl_cd'
                         )

    logging.info('Selecting duplicates in transactions')
    df8 = df7_1.groupBy('crnt_acct_trx_key').agg(count('crnt_acct_trx_key').alias('count'))
    df8 = df8.filter(df8['count'] > 1)

    logging.info('Missed transactions')
    df9 = df7_1.select('crnt_acct_trx_key', 'dbc_key')
    df9 = df9.filter(df9.dbc_key.isNotNull()).select('crnt_acct_trx_key').dropDuplicates()

    df9.show()

    logging.info('Accurately identified')
    df10 = df7_2.filter(df7_2.dbc_key.isNotNull()).join(df8, ['crnt_acct_trx_key'], 'left')
    df10 = df10.select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'acct_key', 'bank_id', 'kir_branch_id', 'contract_id'
                       , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key', 'main_acct_id', 'prd_key', 'prd_type_cd'
                       , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt', 'chnl_cd', 'trx_cd', 'acc_type_num',
                       'short_title'
                       , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt', 'trx_amt_pln', 'trx_amt_cur',
                       'bal_amt_pln'
                       , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id', 'sender_orig_acct_id', 'sender_nm_address'
                       , 'receiver_acct_key', 'receiver_acct_id', 'receiver_orig_acct_id', 'receiver_nm_address'
                       , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts', 'tech_acc_dt'
                       , 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm', 'trx_auth_type_cd',
                       'trx_address_nm'
                       , 'trx_device_cd', 'dbc_chnl_cd')
    df10.show()

    logging.info('Left Joining duplicates with (crnt + dbc) to try assign MCC')
    df11 = df6_mcc.join(df8, ['crnt_acct_trx_key'], 'left').join(df9, ['crnt_acct_trx_key'], 'left')

    a11 = df11.alias('a11')
    a5 = df5.alias('a5')

    logging.info('Joining Second Grouping dbc with crnt_acct_trxs')
    df12 = df11.join(df5, (a11.acct_key == a5.acct_key) & (a11.value_dt == a5.dbc_trx_dt) & (
                a11.mod_trx_amt_pln == a5.sum_coalesce_trx), 'left')
    df12 = df12.select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'mcc.acct_key', 'bank_id', 'kir_branch_id',
                       'contract_id'
                       , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key', 'main_acct_id', 'prd_key', 'prd_type_cd'
                       , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt', 'chnl_cd', 'trx_cd', 'acc_type_num',
                       'short_title'
                       , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt', 'trx_amt_pln', 'trx_amt_cur',
                       'bal_amt_pln'
                       , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id', 'sender_orig_acct_id', 'sender_nm_address'
                       , 'receiver_acct_key', 'receiver_acct_id', 'receiver_orig_acct_id', 'receiver_nm_address'
                       , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts', 'tech_acc_dt'
                       , 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm',
                       col('max_trx_auth_type_cd').alias('trx_auth_type_cd'),
                       col('max_trx_address_nm').alias('trx_address_nm'),
                       col('max_trx_device_cd').alias('trx_device_cd'), 'dbc_chnl_cd', 'dopisz_mcc')

    df12.show()

    logging.info('Duplicates')
    df13 = df12.groupBy('crnt_acct_trx_key').agg(count('crnt_acct_trx_key').alias('count13'))
    df13 = df13.filter(df13['count13'] > 1)
    df13.show()

    logging.info('Missed')
    df14 = df12.select('crnt_acct_trx_key', 'dbc_key').filter(df12.dbc_key.isNull()).dropDuplicates()
    a14 = df14.alias("a14")
    df14.show()

    logging.info('Preparing Non Missed')
    df15 = df12.filter(df12.dbc_key.isNotNull()).join(df13, ['crnt_acct_trx_key'], 'left')
    df15 = df15.filter(df15.count13.isNotNull())
    df15 = df15.select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'acct_key', 'bank_id', 'kir_branch_id', 'contract_id'
                       , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key', 'main_acct_id', 'prd_key', 'prd_type_cd'
                       , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt', 'chnl_cd', 'trx_cd', 'acc_type_num',
                       'short_title'
                       , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt', 'trx_amt_pln', 'trx_amt_cur',
                       'bal_amt_pln'
                       , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id', 'sender_orig_acct_id', 'sender_nm_address'
                       , 'receiver_acct_key', 'receiver_acct_id', 'receiver_orig_acct_id', 'receiver_nm_address'
                       , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts', 'tech_acc_dt'
                       , 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm', 'trx_auth_type_cd',
                       'trx_address_nm'
                       , 'trx_device_cd', 'dbc_chnl_cd')

    df15.write.format('bigquery') \
        .mode("append")\
        .option('table', dpcrnt_acct_trx_fcd) \
        .save()


    df15.show()

    df16 = df6_mcc.join(df13, ['crnt_acct_trx_key'], 'left').join(df14, ['crnt_acct_trx_key'], 'left')
    df16 = df16.filter(df16.count13.isNotNull())
    df16.show()

    a16 = df16.alias("a16")
    a4 = df4.alias("a4")
    a16.show()
    a4.show()
    df17 = df16.join(df4, ['acct_key'], 'left').drop(a4.dbc_key)
    df17 = df17.select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'mcc.acct_key', 'bank_id', 'kir_branch_id',
                       'contract_id'
                       , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key', 'main_acct_id', 'prd_key', 'prd_type_cd'
                       , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt', 'mcc.chnl_cd', 'trx_cd', 'acc_type_num',
                       'short_title'
                       , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt', 'trx_amt_pln', 'trx_amt_cur',
                       'bal_amt_pln'
                       , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id', 'sender_orig_acct_id', 'sender_nm_address'
                       , 'receiver_acct_key', 'receiver_acct_id', 'receiver_orig_acct_id', 'receiver_nm_address'
                       , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts', 'tech_acc_dt'
                       , 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd', 'trx_city_nm',
                       col('max_trx_auth_type_cd').alias('trx_auth_type_cd'),
                       col('max_trx_address_nm').alias('trx_address_nm'),
                       col('max_trx_device_cd').alias('trx_device_cd'), 'dbc_chnl_cd', 'dopisz_mcc')
    df17.show()

    df18 = df17.select('crnt_acct_trx_key').groupBy('crnt_acct_trx_key').agg(
        count('crnt_acct_trx_key').alias('count18'))
    df18 = df18.filter(df18['count18'] > 1)

    df19 = df17.select('crnt_acct_trx_key', 'dbc_key').filter(df17.dbc_key.isNull()).dropDuplicates()

    df20 = df17.filter(df17.dbc_key.isNotNull()).join(df18, ['crnt_acct_trx_key'], 'left')
    df20 = df20.filter(df20.count18.isNotNull()).select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'acct_key', 'bank_id',
                                                        'kir_branch_id', 'contract_id'
                                                        , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key',
                                                        'main_acct_id', 'prd_key', 'prd_type_cd'
                                                        , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt',
                                                        'mcc.chnl_cd', 'trx_cd', 'acc_type_num', 'short_title'
                                                        , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt',
                                                        'trx_amt_pln', 'trx_amt_cur', 'bal_amt_pln'
                                                        , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id',
                                                        'sender_orig_acct_id', 'sender_nm_address'
                                                        , 'receiver_acct_key', 'receiver_acct_id',
                                                        'receiver_orig_acct_id', 'receiver_nm_address'
                                                        , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts',
                                                        'tech_acc_dt'
                                                        , 'mcc_cd', 'dbc_key', 'dbc_id', 'trx_country_cd',
                                                        'trx_city_nm', 'trx_auth_type_cd', 'trx_address_nm'
                                                        , 'trx_device_cd', 'dbc_chnl_cd')
    df20.write.format('bigquery') \
        .mode("append") \
        .option('table', dpcrnt_acct_trx_fcd) \
        .save()

    df21 = df2_mcc.join(df18, ['crnt_acct_trx_key'], 'left'). \
        join(df19, ['crnt_acct_trx_key'], 'left') \
        .join(df6_no_mcc, ['crnt_acct_trx_key'], 'left')
    df21 = df21.filter(df21.count18.isNotNull()).select('crnt_acct_trx_key', 'due_dt', 'sys_cd', 'acct_key', 'bank_id',
                                                        'kir_branch_id', 'contract_id'
                                                        , 'acct_id', 'trx_num', 'frst_acct_id', 'frst_acct_key',
                                                        'main_acct_id', 'prd_key', 'prd_type_cd'
                                                        , 'prd_id', 'subprd_id', 'trx_ts', 'trx_dt', 'acc_dt',
                                                        'chnl_cd', 'trx_cd', 'acc_type_num', 'short_title'
                                                        , 'title', 'acct_cur_cd', 'cur_cd', 'value_dt', 'trx_cur_rt',
                                                        'trx_amt_pln', 'trx_amt_cur', 'bal_amt_pln'
                                                        , 'bal_amt_cur', 'sender_acct_key', 'sender_acct_id',
                                                        'sender_orig_acct_id', 'sender_nm_address'
                                                        , 'receiver_acct_key', 'receiver_acct_id',
                                                        'receiver_orig_acct_id', 'receiver_nm_address'
                                                        , 'tech_etl_pkg_cd', 'tech_insert_id', 'tech_insert_ts',
                                                        'tech_acc_dt')
    df21.write.format('bigquery') \
        .mode("append") \
        .option('table', dpcrnt_acct_trx_fcd) \
        .save()

    df21.show()

    spark.stop()


if __name__ == "__main__":
    main()