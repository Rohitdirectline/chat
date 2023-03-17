"""Glue IN_RADAR Initial Load Ingestion Job."""
# pylint: disable=line-too-long
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import datetime
import json
import os  # pragma: no cover
import re
import sys  # pragma: no cover
import threading
import time
import traceback

import boto3
import pyspark.sql.functions as f
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job  # pragma: no cover
from awsglue.utils import getResolvedOptions  # pragma: no cover
from pyspark.sql.window import Window
from shredding.config.param import param_config
from shredding.jobs.shredding import shred, shred_hist_df
from shredding.utils.helper import (PropagatingThread, config_schema,
                                    create_message, dump_failed_records,
                                    ext_from_xml, ext_schema_of_xml_df,
                                    flatten_df, get_spark_env, rem_col_str,
                                    send_email)
from shredding.utils.logger import Log

logger = Log()
sys.path.insert(0, os.path.dirname(__file__))  # pragma: no cover

# JOB_TYPE = "initial_load"   # pragma: no cover


if __name__ == "__main__":  # pragma: no cover

    # -----------  G E T  S P A R K  E N V I R O N M E N T   ---------------
    (
        conf,
        sc,
        sql_context,
        glue_context,
        spark_session,
    ) = get_spark_env()  # pragma: no cover
    dev = boto3.session.Session()
    glue_client = dev.client("glue")
    s3_client = dev.client("s3")
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "source",
            "connectionName",
            "topic_name",
            "reprocess_table_list",
            "reprocess_partition",
            "env",
        ],
    )

    
    job_name = args["JOB_NAME"]
    job_run_id = args['JOB_RUN_ID']

    source = args["source"]
    connectionName = args["connectionName"]
    topic_name = args["topic_name"]
    env = args["env"]
    topic_arn = param_config["SNS"][env]["topic_arn"]
   

    glue_job = Job(glue_context)  # pragma: no cover
    glue_job.init(job_name, args)  # pragma: no cover

    dev = boto3.session.Session()
    glue_client = dev.client("glue")
    sns_client = dev.client("sns")
    ##### SELECTING SOURCE ##########

    if source == "kafka":
        dataframe_KafkaStream_node1 = glue_context.create_data_frame.from_options(
            connection_type="kafka",
            connection_options={
                "connectionName": connectionName,
                "classification": "json",
                "startingOffsets": "latest",
                "topicName": topic_name,
                "inferSchema": "true",
                "failOnDataLoss": "false",
                "typeOfData": "kafka",
            },
            transformation_ctx="dataframe_KafkaStream_node1",
        )

        # -----------  creating process batch function for topic  ---------------

        def processBatch(data_frame, batchId):

            # batch id
            batch_id = datetime.datetime.today().strftime("%Y%m%d%H%M%S%s")

            if data_frame.count() > 0:

                try:
                    start_time = time.time()

                    # Generating df
                    KafkaStream_node1 = DynamicFrame.fromDF(
                        data_frame, glue_context, "from_data_frame"
                    )

                    # conversion of glue df to spark df
                    df = KafkaStream_node1.toDF()

                    # Reading config file for the topic to get list of output tables

                    table_list = param_config["Topic"][env][topic_name]["table_list"]

                    df = df.withColumnRenamed("_corrupt_record", "value").select(
                        "value"
                    )

                    # Generating schema for the xml
                    payload_schema = ext_schema_of_xml_df(
                        df.select("value"),
                        spark_session=spark_session,
                        options={
                            "inferSchema": "false",
                            "rowTag": "request",
                            "valueTag": "Remove_me",
                            "ignoreNamespace": "true",
                        },
                    )

                    # Parsing xml schema to df
                    df = df.withColumn(
                        "parsed",
                        ext_from_xml(
                            df.value,
                            schema=payload_schema,
                            spark_session=spark_session,
                            options={
                                "inferSchema": "false",
                                "rowTag": "request",
                                "valueTag": "Remove_me",
                                "ignoreNamespace": "true",
                            },
                        ),
                    ).select("parsed")

                    # Flattening first level of df to separate header and payload quote
                    df = flatten_df(df)

                    # Renaming columns to support config
                    cols = [
                        f.col(s).alias(rem_col_str(s, value="parsed-", replace=""))
                        for s in df.columns
                    ]
                    df = df.select(cols)

                    # Caching the dataframe
                    df.cache()
                    logger.info(f"Count of raw dataframe {df.count()} ")

                    # Reading config schema from glue registry
                    logger.info("Start reading the schema from aws registry")
                    table_schema_ver_conf = param_config["table_schema_version"][env]
                    schema_config = config_schema(
                        glue_client=glue_client,
                        table_list=table_list,
                        config=param_config,
                        table_schema_ver_conf = table_schema_ver_conf
                    )
                    logger.info(f"Reading schema is completed {schema_config}")

                    threads = []
                    for table in table_list:
                        threads.append(
                            PropagatingThread(
                                target=shred,
                                kwargs={
                                    "df": df,
                                    "raw_df": data_frame,
                                    "glue_client": glue_client,
                                    "table_name": table,
                                    "spark_session": spark_session,
                                    "glue_context": glue_context,
                                    "param_config": param_config,
                                    "schema_config": schema_config,
                                    "source": "kafka",
                                    "env": env,
                                    "batch_id": batch_id,
                                    "topic_name": topic_name,
                                    "s3_client": s3_client,
                                    "reprocess_flag": False,
                                    "sns_client": sns_client,
                                    "job_name":job_name,
                                    "job_run_id":job_run_id
                                },
                            ),
                        )

                    pools = param_config["Topic"][env]['pools']
                    n = len(pools)
                    thread_chunks = [
                        threads[i : i + n] for i in range(0, len(threads), n)
                    ]

                    for threads in thread_chunks:

                        for i in range(len(threads)):
                            sc.setLocalProperty("spark.scheduler.pool", pools[i])
                            threads[i].start()

                        for i in range(len(threads)):
                            threads[i].join()

                    logger.info(
                        f" Time taken to process batch {batch_id}--- {time.time() - start_time} seconds ---"
                    )

                    ## Garbage collection of threads
                    threads, thread_chunks = [], []

                    logger.info(
                        f" Time taken to process batch {batch_id}--- {time.time() - start_time} seconds ---"
                    )

                except Exception as error:
                    error = repr(error)
                    logger.error(f"Error has occurred outside shredding logic {error}")
                    var = traceback.format_exc()
                    logger.error(var)
                    logger.info(traceback.print_exc())
                    error_w_trace = error + " " + var

                    ### Creating message for notification
                    message, subject = create_message(
                        df=df,
                        raw_df=data_frame,
                        table_name=None,
                        batch_id=batch_id,
                        topic_name=topic_name,
                        error_message=error,
                        job_name=job_name,
                        job_run_id=job_run_id,
                        param_config=param_config,
                        env=env,
                        failure="unshredded",
                    )
                    ### Sending email for failure
                    send_email(
                        err_msg=message,
                        topic_arn=topic_arn,
                        subject=subject,
                        sns_client=sns_client,
                    )

                    #### Dumping failed records to s3
                    dump_failed_records(
                        table_name=None,
                        df=None,
                        raw_df=data_frame,
                        param_config=param_config,
                        env=env,
                        batch_id=batch_id,
                        topic_name=topic_name,
                        spark_session=spark_session,
                        error=error_w_trace,
                        failure="unshredded",
                        glue_context=glue_context,
                        glue_client=glue_client,
                    )

            else:
                logger.info(data_frame.show())
                logger.info(f" Data frame has zero count for batchid {batch_id}")

        # -----------  P R O C E S S  D A T A   ---------------
        glue_context.forEachBatch(
            frame=dataframe_KafkaStream_node1,
            batch_function=processBatch,
            options={
                "windowSize": "15 seconds",
                "checkpointLocation": args["TempDir"]
                + "/"
                + args["JOB_NAME"]
                + "/checkpoint/",
            },
        )

    elif source == "audit":
        start_time = time.time()
        reprocess_table_list = args["reprocess_table_list"]
        reprocess_table_list = reprocess_table_list.split(",")
        reprocess_partition = args["reprocess_partition"]
        batch_id = datetime.datetime.today().strftime("%Y%m%d%H%M%S%s")
        reprocess_partition = re.findall(r"\d+", reprocess_partition)
        dal_year = reprocess_partition[0]
        dal_month = reprocess_partition[1]
        dal_day = reprocess_partition[2]
        audit_db = param_config["audit"][env]["audit_db"]
        audit_table_name = param_config["audit"][env]["audit_table_name"]
        shredded_failed_table = param_config["audit"][env]["shredded_failed_table"]

        logger.info(f"reprocess_table_list is {reprocess_table_list}")

        audit_df = (
            spark_session.read.table(f"{audit_db}.{audit_table_name}")
            .filter(f"topic_name = '{topic_name}' ")
            .filter(f"dal_year='{dal_year}'")
            .filter(f"dal_month='{dal_month}'")
            .filter(f"dal_day='{dal_day}'")
            .filter(f.col("table_name").isin(reprocess_table_list))
        )
        audit_df = audit_df.withColumn('salt', f.rand()).repartition(200, 'salt')

        ### Selecting only latest processed metadata for each table
        window_spec = Window.partitionBy(
            "correlationid", "filename", "table_name"
        ).orderBy(f.col("shredding_timestamp").desc())
        audit_df = (
            audit_df.withColumn("window_id", f.dense_rank().over(window_spec))
            .filter("window_id=1")
            .filter("processed_flag=false")
            .select("correlationid", "filename", "table_name", "schema_version_number")
            .drop_duplicates()
        )
        ### Reading shredded failure table
        failed_df = (
            spark_session.read.table(f"{audit_db}.{shredded_failed_table}")
            .filter(f"topic_name = '{topic_name}' ")
            .filter(f"dal_year='{dal_year}'")
            .filter(f"dal_month='{dal_month}'")
            .filter(f"dal_day='{dal_day}'")
            .filter(f.col("table_name").isin(reprocess_table_list))
            .select("value", "filename", "correlationid")
            .drop_duplicates()
        )
        audit_df = (
            audit_df.join(failed_df, on=["correlationid", "filename"], how="left")
            .filter("value is not null")
            .select("table_name", "value", "schema_version_number")
        )

        if audit_df.count()==0:
            
            logger.info(
                f" No records are present to reprocess {reprocess_table_list}"
            )
            sys.exit(0)
        ### Creating index column
        audit_df = (
            audit_df.rdd.zipWithIndex()
            .toDF()
            .select(f.col("_1.*"), f.col("_2").alias("increasing_id"))
        )
        # audit_df.cache()
        # count = audit_df.count()
        logger.info(f"Total number of failed_records that will be processed = {audit_df.count()}")

        max_id = audit_df.agg({"increasing_id": "max"}).collect()[0][0]

        step = 100000
        for i in range(0, max_id, step):
            df = audit_df.filter(
                (f.col("increasing_id") >= i) & (f.col("increasing_id") <= i + step - 1)
            )
            logger.info(
                f"Number of records in a batch {i} that will be processed {df.count()}"
            )

            schema_version_numbers = [
                j
                for i in df.select("schema_version_number").distinct().collect()
                for j in i
            ]
            for schema_version in schema_version_numbers:
                # Reading config schema from glue registry
                logger.info("Start reading the schema from aws registry")
                schema_config = config_schema(
                    glue_client=glue_client,
                    table_list=reprocess_table_list,
                    config=param_config,
                    schema_version_number=schema_version,
                )
                logger.info(f"Reading schema is completed {schema_config}")

                master_df = (
                    df.filter(f"schema_version_number='{str(schema_version)}'")
                    .select("value")
                    .drop_duplicates()
                )

                master_df = shred_hist_df(
                    df=master_df, source="unshredded", spark_session=spark_session
                )

                master_df.cache()
                master_df.count()
                
                threads = []
                for table in reprocess_table_list:
                    threads.append(
                        PropagatingThread(
                            target=shred,
                            kwargs={
                                "df": master_df,
                                "raw_df": df.select("value"),
                                "glue_client": glue_client,
                                "table_name": table,
                                "spark_session": spark_session,
                                "glue_context": glue_context,
                                "param_config": param_config,
                                "schema_config": schema_config,
                                "source": "audit",
                                "env": env,
                                "batch_id": batch_id,
                                "topic_name": topic_name,
                                "s3_client": s3_client,
                                "reprocess_flag": False,
                                "sns_client": sns_client,
                                "job_name":job_name,
                                "job_run_id":job_run_id
                            },
                        ),
                    )

                pools = param_config["Topic"][env]['pools']
                n = len(pools)
                thread_chunks = [threads[i : i + n] for i in range(0, len(threads), n)]

                for threads in thread_chunks:

                    for i in range(len(threads)):
                        sc.setLocalProperty("spark.scheduler.pool", pools[i])
                        threads[i].start()

                    for i in range(len(threads)):
                        threads[i].join()

                logger.info(
                    f" Time taken to process batch {batch_id}--- {time.time() - start_time} seconds ---"
                )

                ## Garbage collection of threads
                threads, thread_chunks = [], []

                logger.info(
                    f" Time taken to process batch {batch_id}--- {time.time() - start_time} seconds ---"
                )
                thread_chunks, threads = [], []

        logger.info(
            f"Reprocessing  Completed of unshredded data for {reprocess_table_list}"
        )

        # Logic will be written to fix audit table
    elif source == "unshredded":
        start_time = time.time()
        reprocess_table_list = param_config["Topic"][env][topic_name]["table_list"]
        # reprocess_table_list = ["in_radar_device"]
        reprocess_partition = args["reprocess_partition"]
        batch_id = datetime.datetime.today().strftime("%Y%m%d%H%M%S%s")
        reprocess_partition = re.findall(r"\d+", reprocess_partition)
        dal_year = reprocess_partition[0]
        dal_month = reprocess_partition[1]
        dal_day = reprocess_partition[2]
        audit_db = param_config["audit"][env]["audit_db"]
        audit_table_name = param_config["audit"][env]["audit_table_name"]
        unshredded_failed_table = param_config["audit"][env]["unshredded_failed_table"]

        logger.info(f"reprocess_table_list is {reprocess_table_list}")

        unshred_df = (
            spark_session.read.table(f"{audit_db}.{unshredded_failed_table}")
            .filter(f"topic_name = '{topic_name}' ")
            .filter(f"dal_year='{dal_year}'")
            .filter(f"dal_month='{dal_month}'")
            .filter(f"dal_day='{dal_day}'")
            .select("schema_version_config", "correlationid", "filename", "value")
        )
        
        unshred_df = unshred_df.withColumn('salt', f.rand()).repartition(200, 'salt')

        audit_df = (
            spark_session.read.table(f"{audit_db}.{audit_table_name}")
            .filter(f"topic_name = '{topic_name}' ")
            .filter(f"dal_year='{dal_year}'")
            .filter(f"dal_month='{dal_month}'")
            .filter(f"dal_day='{dal_day}'")
            .filter("processed_flag=true").select("correlationid","filename").distinct()
        )
        unshred_df = unshred_df.join(
            audit_df, on=["correlationid", "filename"], how="left_anti"
        ).select("value", "schema_version_config")

        ### Creating index column
        unshred_df = (
            unshred_df.rdd.zipWithIndex()
            .toDF()
            .select(f.col("_1.*"), f.col("_2").alias("increasing_id"))
        )
        
        max_id = unshred_df.agg({"increasing_id": "max"}).collect()[0][0]

        logger.info(f"Total number of unshredded records to be processed  = {unshred_df.count()}")

        step = 100000
        for i in range(0, max_id, step):
            df = unshred_df.filter(
                (f.col("increasing_id") >= i) & (f.col("increasing_id") <= i + step - 1)
            )
            logger.info(
                f"Number of records in a batch {i} that will be processed {df.count()}"
            )

            schema_version_config = [
                j
                for i in df.select("schema_version_config").distinct().collect()
                for j in i
            ]
            logger.info(f"schema_version_config is {schema_version_config}")

            for svc in schema_version_config:
                svc_json = json.loads(svc)

                # Reading config schema from glue registry
                logger.info("Start reading the schema from aws registry")
                schema_config = config_schema(
                    glue_client=glue_client,
                    table_list=reprocess_table_list,
                    config=param_config,
                    table_schema_ver_conf=svc_json,
                )

                logger.info(f"Reading schema is completed {schema_config}")

                master_df = df.filter(
                    f.col("schema_version_config").isin(svc.split("#"))
                ).select("value")

                master_df = shred_hist_df(
                    df=master_df, source="unshredded", spark_session=spark_session
                )
                master_df.cache()
                master_df.count()

                threads = []
                for table in reprocess_table_list:
                    threads.append(
                        PropagatingThread(
                            target=shred,
                            kwargs={
                                "df": master_df,
                                "raw_df": df.select("value"),
                                "glue_client": glue_client,
                                "table_name": table,
                                "spark_session": spark_session,
                                "glue_context": glue_context,
                                "param_config": param_config,
                                "schema_config": schema_config,
                                "source": "unshredded",
                                "env": env,
                                "batch_id": batch_id,
                                "topic_name": topic_name,
                                "s3_client": s3_client,
                                "reprocess_flag": False,
                                "sns_client": sns_client,
                                "job_name":job_name,
                                "job_run_id":job_run_id
                            },
                        ),
                    )

                pools = param_config["Topic"][env]['pools']
                n = len(pools)
                thread_chunks = [threads[i : i + n] for i in range(0, len(threads), n)]

                for threads in thread_chunks:

                    for i in range(len(threads)):
                        sc.setLocalProperty("spark.scheduler.pool", pools[i])
                        threads[i].start()

                    for i in range(len(threads)):
                        threads[i].join()

                logger.info(
                    f" Time taken to process batch {batch_id}--- {time.time() - start_time} seconds ---"
                )

                ## Garbage collection of threads
                threads, thread_chunks = [], []


        logger.info(
            f"Reprocessing  Completed of unshredded data for {reprocess_table_list}"
        )
    logger.info(f"Job is completed")
    # -----------  S U B M I T  G L U E  J O B   ---------------
    glue_job.commit()
