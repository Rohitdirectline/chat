# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import base64
import datetime
import hashlib
import json
import sys
from collections import ChainMap, OrderedDict
from operator import getitem

import boto3
import pyspark.sql.functions as f
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from dateutil.relativedelta import relativedelta
from pyspark import InheritableThread
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, Window
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    _parse_datatype_json_string,
)
from pyspark.sql.utils import AnalysisException
from shredding.config.param import windows_conf
from shredding.utils.logger import Log

logger = Log()

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session


def config_schema(
    table_list,
    config,
    glue_client,
    table_schema_ver_conf=None,
    schema_version_number=None,
):
    """_summary_
    Args:
        table_name (_type_): _description_
        SchemaName (_type_): _description_
        RegistryName (_type_): _description_
        version_number (int, optional): _description_. Defaults to 1.
        glue_client (_type_, optional): _description_. Defaults to None.
    Returns:
        _type_: _description_
    """
    conf_list = []
    registry_name = config["registry_name"]
    if not table_schema_ver_conf:
        table_schema_ver_conf = config["table_schema_version"]

    for table in table_list:
        if schema_version_number:
            version_number = int(schema_version_number)
        else:
            version_number = table_schema_ver_conf[table]
        schema_message = glue_client.get_schema_version(
            SchemaId={
                # 'SchemaArn': 'string',
                "SchemaName": f"{table}",
                "RegistryName": f"{registry_name}",
            },
            # SchemaVersionId='string',
            SchemaVersionNumber={
                "LatestVersion": False,
                "VersionNumber": version_number,
            },
        )
        schema = schema_message["SchemaDefinition"]
        schema = json.loads(schema)
        conf_list.append(schema)
    final_config = dict(ChainMap(*conf_list))
    return final_config


def ext_from_xml(xml_column, schema, spark_session, options={}):
    """_summary_

    Args:
        xml_column (_type_): _description_
        schema (_type_): _description_
        spark_session (_type_): _description_
        options (dict, optional): _description_. Defaults to {}.

    Returns:
        _type_: _description_
    """

    try:

        java_column = _to_java_column(xml_column.cast("string"))
        java_schema = spark_session._jsparkSession.parseDataType(schema.json())
        scala_map = (
            spark_session._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(
                options
            )
        )
        jc = spark_session._jvm.com.databricks.spark.xml.functions.from_xml(
            java_column, java_schema, scala_map
        )
        return Column(jc)

    except Exception as e:
        logger.error(f" Error has occured in parsing xml schema {e}")
        # Send email
        # Call dump function


def ext_schema_of_xml_df(df, spark_session, options={}):
    """_summary_

    Args:
        df (_type_): _description_
        spark_session (_type_): _description_
        options (dict, optional): _description_. Defaults to {}.

    Returns:
        _type_: _description_
    """

    try:
        assert len(df.columns) == 1

        scala_options = spark_session._jvm.PythonUtils.toScalaMap(options)
        java_xml_module = getattr(
            getattr(spark_session._jvm.com.databricks.spark.xml, "package$"), "MODULE$"
        )
        java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
        return _parse_datatype_json_string(java_schema.json())

    except Exception as e:
        logger.error(f" Error has occured in generating xml schema {e}")
        # Send email
        # Call dump function


def type_cols(df_dtypes, filter_type):
    """_summary_

    Args:
        df_dtypes (_type_): _description_
        filter_type (_type_): _description_

    Returns:
        _type_: _description_
    """
    cols = []
    for col_name, col_type in df_dtypes:
        if col_type.startswith(filter_type):
            cols.append(col_name)
    return cols


def flatten_df(nested_df, sep="-"):
    """_summary_

    Args:
        nested_df (_type_): _description_
        sep (str, optional): _description_. Defaults to "-".

    Returns:
        _type_: _description_
    """
    nested_cols = type_cols(nested_df.dtypes, "struct")
    flatten_cols = [fc for fc, _ in nested_df.dtypes if fc not in nested_cols]
    for nc in nested_cols:
        for cc in nested_df.select(f"{nc}.*").columns:
            if sep is None:
                flatten_cols.append(f.col(f"{nc}.{cc}").alias(f"{cc}"))
            else:
                flatten_cols.append(f.col(f"{nc}.{cc}").alias(f"{nc}{sep}{cc}"))
    return nested_df.select(flatten_cols)


def explode_df(nested_df):
    """_summary_

    Args:
        nested_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    nested_cols = type_cols(nested_df.dtypes, "array")
    exploded_df = nested_df
    for nc in nested_cols:
        exploded_df = exploded_df.withColumn(nc, f.explode(f.col(nc)))
    return exploded_df


def drop_remove_me_n_value_cols(in_df):
    """_summary_

    Args:
        in_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    out_df = flatten_df(in_df)
    columns_to_keep = []
    for name in out_df.columns:
        if "REMOVE_ME" not in name.upper():
            columns_to_keep.append(name)

    out_df = out_df.select(columns_to_keep)
    new_column_name_list = [x.replace("__value", "") for x in out_df.columns]

    return out_df.toDF(*new_column_name_list)


def nested_flatten(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    nested_cols = type_cols(df.dtypes, "struct")
    nested_array_cols = type_cols(df.dtypes, "array")
    while nested_cols or nested_array_cols:
        while nested_cols:
            df = drop_remove_me_n_value_cols(df)
            nested_cols = type_cols(df.dtypes, "struct")
        nested_array_cols = type_cols(df.dtypes, "array")
        while nested_array_cols:
            df = explode_df(df)
            nested_array_cols = type_cols(df.dtypes, "array")

        nested_cols = type_cols(df.dtypes, "struct")
        nested_array_cols = type_cols(df.dtypes, "array")

    return df


def has_column(df, col):
    """_summary_

    Args:
        df (_type_): _description_
        col (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        df.select(col)
        return True
    except AnalysisException:
        return False


def rem_col_str(s, value=".", replace="_"):
    """_summary_

    Args:
        s (_type_): _description_
        value (str, optional): _description_. Defaults to ".".
        replace (str, optional): _description_. Defaults to "_".

    Returns:
        _type_: _description_
    """

    return s.replace(value, replace)


def remove_prefix(s, prefix):
    """_summary_

    Args:
        s (_type_): _description_
        prefix (_type_): _description_

    Returns:
        _type_: _description_
    """
    return s[len(prefix) :] if s.startswith(prefix) else s


def check_cols(config, table_name, filename_source, source):
    """_summary_

    Args:
        df (_type_): _description_
        config (_type_): _description_
        table_name (_type_): _description_
        filename_source (_type_): _description_
        source (_type_): _description_

    Returns:
        _type_: _description_
    """

    table_conf = config[str(table_name)]
    window_function_cols = []
    par_heir_cols = []
    child_cols = []

    # Selecting source columns from config
    for key, value in table_conf.items():
        source_cols = value["source_columns"]
        parent_flag = value["parent_hierarchy_flag"]
        if "window_function_row" in source_cols:
            window_function_cols.append(key)

        elif parent_flag:
            par_heir_cols.append(source_cols)

        else:
            child_cols.append(source_cols)

    # print(child_cols)
    child_cols = list(set([j for i in child_cols for j in i]))

    par_heir_cols = list(set([j for i in par_heir_cols for j in i]))

    # Adding fileName column
    if filename_source not in child_cols:
        child_cols.append(filename_source)

    if source in ["kafka", "audit", "unshredded"]:

        return child_cols, par_heir_cols, window_function_cols

    if source == "bucket":
        ### Mapping headers columns to the output table
        header_map = {}
        for key, value in config["quote_header"].items():
            value = [i for i in value["source_columns"]]

            header_map[value[0]] = key
        # Removing prefix as bucket xml start with root tag
        child_cols = [remove_prefix(s, prefix="payload.root.") for s in child_cols]
        # Removing columns with header prefix as bucket xml start with root tag
        child_cols = [
            header_map[i] if i.startswith("header.") else i for i in child_cols
        ]
        # child_cols = [x for x in child_cols if not x.startswith("header.")]

        par_heir_cols = [
            remove_prefix(s, prefix="payload.root.") for s in par_heir_cols
        ]
        # Removing header prefix as bucket xml start with root tag
        par_heir_cols = [
            i.split(".")[-1] if i.startswith("header.") else i for i in par_heir_cols
        ]

    return child_cols, par_heir_cols, window_function_cols


def pres_or_not_column(df, col_list):
    """_summary_

    Args:
        df (_type_): _description_
        col_list (_type_): _description_

    Returns:
        _type_: _description_
    """
    present_cols = []
    not_present_cols = []

    for column in col_list:
        if has_column(df, column):
            present_cols.append(column)
        else:
            not_present_cols.append(column)
    return present_cols, not_present_cols


def mapping_cols(config, table_name, source):
    """_summary_

    Args:
        config (_type_): _description_
        table_name (_type_): _description_
        source (_type_): _description_
    """

    table_conf = config[str(table_name)]
    output_cols = []
    source_cols = []
    mapped_cols = []
    if source in ["kafka", "audit", "unshredded"]:

        for key, val in table_conf.items():
            source_val = val["source_columns"]
            if "window_function_row" not in source_val:
                cols = [i.replace(".", "-") for i in source_val]
                # df=df.withColumn(key,f.concat_ws(sep, *cols))
                output_cols.append(key)
                source_cols.append(cols)
            else:
                cols = [i.replace(".", "-") for i in source_val]
                # df=df.withColumn(key,f.concat_ws(sep, *cols))
                output_cols.append(key)
                source_cols.append([key])

    elif source == "bucket":
        header_map = {}
        for key, value in config["quote_header"].items():
            value = [i for i in value["source_columns"]]
            header_map[value[0]] = key
        for key, val in table_conf.items():

            source_val = val["source_columns"]

            # Removing payload.root. prefix as bucket xml start with root tag
            source_val = [remove_prefix(s, prefix="payload.root.") for s in source_val]

            # Removing header prefix as bucket xml start with root tag
            source_val = [
                header_map[i] if i.startswith("header.") else i for i in source_val
            ]

            cols = [i.replace(".", "-") for i in source_val]
            # df=df.withColumn(key,f.concat_ws(sep, *cols))
            output_cols.append(key)
            source_cols.append(cols)

    mapping = dict(zip(output_cols, source_cols))

    if source in ["kafka", "audit", "unshredded"]:
        if not mapping.get("filename"):
            mapping.update({"filename": ["header-fileName"]})
    elif source == "bucket":
        if not mapping.get("filename"):
            mapping.update({"filename": ["filename"]})

    for key, value in mapping.items():

        ### For combination of keys concat multiple columns
        if len(value) > 1:
            mapped_cols.append(f.concat_ws("_", *value).alias(key))
        ### For single one to one mapping
        elif len(value) == 1:
            mapped_cols.append(f.col(value[0]).alias(key))

    return mapped_cols


def decode_xml(value):
    """_summary_

    Args:
        value (_type_): _description_

    Returns:
        _type_: _description_
    """

    data = base64.b64decode(value).decode("utf-8")
    return data


def datatype_conv(df, config, table_name):
    """_summary_

    Args:
        df (_type_): _description_
        config (_type_): _description_
        table_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    data_config = config[table_name]
    datatype_dict = {}
    for key, value in data_config.items():
        if value["datatype"].lower() != "string":
            datatype_dict[key] = value["datatype"].lower()
    ### Generating rest of columns for which conv is not required
    rest_cols = [f.col(cl) for cl in df.columns if cl not in datatype_dict]

    ### Generating lis of columns for which conv is  required
    converted_cols = []
    for key, value in datatype_dict.items():
        datatype = str(value).lower()
        if datatype.lower() == "int64":
            converted_cols.append(
                f.coalesce(f.col(key).cast(LongType()), f.lit(None)).alias(key)
            )

        elif datatype.lower() == "int32":
            converted_cols.append(
                f.coalesce(f.col(key).cast(IntegerType()), f.lit(None)).alias(key)
            )

        elif datatype.lower() in ["timestamp", "np.datetime64"]:
            converted_cols.append(
                f.coalesce(f.col(key).cast(TimestampType()), f.lit(None)).alias(key)
            )

        elif datatype.lower() == "float32":
            converted_cols.append(
                f.coalesce(f.col(key).cast(FloatType()), f.lit(None)).alias(key)
            )

        elif datatype.lower() == "double":
            converted_cols.append(
                f.coalesce(f.col(key).cast(DoubleType()), f.lit(None)).alias(key)
            )
        elif datatype.lower() == "decimal":
            converted_cols.append(
                f.coalesce(f.col(key).cast(DecimalType()), f.lit(None)).alias(key)
            )

    return rest_cols, converted_cols


def primary_key_check(df, config, table_name):
    """_summary_

    Args:
        df (_type_): _description_
        config (_type_): _description_
        table_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    pri_keys = []
    for key, value in config[table_name].items():
        if str(value["mandatory"]).lower() == "true":
            pri_keys.append(str(key))
    expression = ""
    if len(pri_keys) > 1:
        for key in pri_keys:
            expression += f"{key} IS NULL OR "
    else:
        expression += f"{key} IS NULL"
    final = df.dropna(subset=pri_keys)
    corrupt = df.filter(expression)
    return final, corrupt, pri_keys


def get_all_keys(prefix, bucket, s3_client, file_ext, notifier=None):
    """function to get keys from the s3 bucket"""
    try:
        all_keys = []
        logger.debug(f"Started fetching all Keys")
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        while True:
            response = s3_client.list_objects_v2(**kwargs)
            if response["KeyCount"] >= 1:
                contents = response["Contents"]
                for content in contents:
                    key = content["Key"]
                    if (
                        key != prefix
                        and key.endswith(file_ext)
                        and key.startswith(prefix)
                    ):
                        all_keys.append(key)
            try:
                kwargs["ContinuationToken"] = response["NextContinuationToken"]
            except KeyError:
                break
        return all_keys

    except Exception as e:
        logger.error(f"Failed due to Error : {e}")
        send_email_notification(2, notifier, e)
        sys.exit()


def get_spark_env():  # pragma: no cover
    """Function to create Spark and Glue contexts."""
    conf = (
        SparkConf()
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.scheduler.allocation.file", "fairscheduler.xml")
    )
    sc = SparkContext(conf=conf)
    # sc.setLogLevel("INF")
    sql_context = SQLContext(sc)
    glue_context = GlueContext(sc)
    spark_session = glue_context.spark_session
    return conf, sc, sql_context, glue_context, spark_session


def send_email_notification(topic, message_body, subject):
    """_summary_

    Args:
        topic (_type_): _description_
        message_body (_type_): _description_
        subject (_type_): _description_
    """
    try:
        client = boto3.client("sns")
        print("inside SNS function")
        client.publish(TopicArn=topic, Message=message_body, Subject=subject)
        logger.info("Successfully Sent Email Notification")
        logger.debug(
            "SNS email notification on topic {} with message body {} and title :{}".format(
                topic, message_body, subject
            )
        )
    except Exception as ce:
        print(f"Exception: {ce}")
        logger.info(f"SNS email notification failed due to error : {ce}")
        logger.debug(
            "SNS email notification failed on topic {} with message body {} and title :{}".format(
                topic, message_body, subject
            )
        )
        error_code = ce.response["Error"]["Code"]
        if error_code in [
            "AccessDeniedException",
            "IncompleteSignature",
            "InternalFailure",
            "AuthorizationError",
        ]:
            logger.error(f"Failed due to Error : {error_code}")
            sys.exit()
        elif error_code in [
            "MissingParameter",
            "RequestExpired",
            "ServiceUnavailable",
            "ThrottlingException",
        ]:
            logger.error(f"Failed due to Error : {error_code}")
            sys.exit()
        else:
            logger.error(f"Failed due to Error : {error_code}")
            sys.exit()


def dump_failed_records(
    table_name,
    df,
    raw_df,
    param_config,
    env,
    batch_id,
    topic_name,
    spark_session,
    error,
    failure,
    glue_context,
    glue_client,
):
    """_summary_

    Args:
        table_name (_type_): _description_
        df (_type_): _description_
        raw_df (_type_): _description_
        audit_config (_type_): _description_
        param_config (_type_): _description_
        env (_type_): _description_
        batch_id (_type_): _description_
        topic_name (_type_): _description_
        spark_session (_type_): _description_
        error (_type_): _description_
        failure (_type_): _description_
        glue_context (_type_): _description_
    """
    logger.info("Dumping failed records to s3 started")
    config = param_config["audit"][env]
    audit_db = config["audit_db"]
    audit_table = config["audit_table_name"]
    unshredded_failed_table = config["unshredded_failed_table"]
    shredded_failed_table = config["shredded_failed_table"]
    # ct stores current time
    ct = datetime.datetime.now()
    dal_year = str(ct.year)
    dal_month = str(ct.month).zfill(2)
    dal_day = str(ct.day).zfill(2)
    dal_hour = str(ct.hour).zfill(2)

    # Adding year,month,day from current time to raw df
    raw_df = DynamicFrame.fromDF(raw_df, glue_context, "from_data_frame")

    # conversion of glue df to spark df
    raw_df = raw_df.toDF()
    raw_df = raw_df.withColumnRenamed("_corrupt_record", "value")
    # Extracting correlationid and filename from xml string
    raw_df = raw_df.withColumn(
        "correlationid",
        f.regexp_extract(
            str=f.col("value"), pattern=r"<correlationid>(.*?)</correlationid>", idx=1
        ),
    ).withColumn(
        "filename",
        f.regexp_extract(
            str=f.col("value"), pattern=r"<fileName>(.*?)</fileName>", idx=1
        ),
    )

    raw_df = (
        raw_df.withColumn("dal_year", f.lit(dal_year).cast("string"))
        .withColumn(
            "dal_month", f.lpad(f.lit(dal_month), len=2, pad="0").cast("string")
        )
        .withColumn("dal_day", f.lpad(f.lit(dal_day), len=2, pad="0").cast("string"))
        .withColumn("dal_hour", f.lpad(f.lit(dal_hour), len=2, pad="0").cast("string"))
        .withColumn("batch_id", f.lit(batch_id).cast("string"))
        .withColumn("topic_name", f.lit(topic_name).cast("string"))
    )

    if failure == "shredded":

        logger.info(f"Writing failed records of {table_name} to audit table")

        schema_version_number = param_config["table_schema_version"][env][table_name]
        df = (
            df.withColumn(
                "source_timestamp", f.col("header.timestamp").cast(TimestampType())
            )
            .withColumn("filename", f.col("header.fileName"))
            .withColumn("correlationid", f.col("header.correlationid"))
            .withColumn("shredding_timestamp", f.lit(ct).cast(TimestampType()))
            .withColumn("dal_year", f.lit(dal_year).cast("string"))
            .withColumn(
                "dal_month", f.lpad(f.lit(dal_month), len=2, pad="0").cast("string")
            )
            .withColumn(
                "dal_day", f.lpad(f.lit(dal_day), len=2, pad="0").cast("string")
            )
            .withColumn(
                "dal_hour", f.lpad(f.lit(dal_hour), len=2, pad="0").cast("string")
            )
            .withColumn("table_name", f.lit(table_name).cast("string"))
            .withColumn(
                "schema_version_number",
                f.lit(str(schema_version_number)).cast("string"),
            )
            .withColumn("batch_id", f.lit(batch_id).cast("string"))
            .withColumn("topic_name", f.lit(topic_name).cast("string"))
            .withColumn("error", f.lit(error).cast("string"))
        )
        df = df.drop_duplicates()
        # Getting audit table columns
        audit_table_cols = spark_session.read.table(f"{audit_db}.{audit_table}").columns
        audit = (
            df.withColumn("processed_flag", f.lit(False))
            .select(audit_table_cols)
            .drop_duplicates()
        )

        # Writing error record in audit table
        table_data, partition_keys = get_current_schema(
            glue_client=glue_client, database_name=audit_db, table_name=audit_table
        )
        s3_path = table_data["table_location"]

        audit.write.mode("append").format("parquet").partitionBy(partition_keys).save(
            f"{s3_path}"
        )
        logger.info(f"Writing failed records of {table_name} to audit table completed")

        ### Adding partition to catalog
        # Generating glue catalog partition for audit table
        logger.info("Updating glue catalog of audit table")
        partition_location = f"{s3_path}/topic_name={topic_name}/table_name={table_name}/dal_year={dal_year}/dal_month={dal_month}/dal_day={dal_day}/dal_hour={dal_hour}"
        partition_values = [
            topic_name,
            table_name,
            dal_year,
            dal_month,
            dal_day,
            dal_hour,
        ]

        generate_partition_glue(
            table_data=table_data,
            glue_client=glue_client,
            partition_location=partition_location,
            partition_values=partition_values,
            database_name=audit_db,
            table_name=audit_table,
        )

        logger.info(
            f"Writing shredded failed records of {table_name} to shredded_failed table"
        )

        table_data, partition_keys = get_current_schema(
            glue_client=glue_client,
            database_name=audit_db,
            table_name=shredded_failed_table,
        )
        s3_path = table_data["table_location"]
        raw_df = raw_df.withColumn("table_name", f.lit(table_name).cast("string"))
        raw_df.write.mode("append").format("parquet").partitionBy(partition_keys).save(
            f"{s3_path}/"
        )
        logger.info(f"Writing completed of {table_name} for shredded_failed table")
        logger.info("Updating glue catalog of shredded_failed table")
        partition_location = f"{s3_path}/topic_name={topic_name}/table_name={table_name}/dal_year={dal_year}/dal_month={dal_month}/dal_day={dal_day}/dal_hour={dal_hour}/batch_id={batch_id}"

        partition_values = [
            topic_name,
            table_name,
            dal_year,
            dal_month,
            dal_day,
            dal_hour,
            batch_id,
        ]

        generate_partition_glue(
            table_data=table_data,
            glue_client=glue_client,
            partition_location=partition_location,
            partition_values=partition_values,
            database_name=audit_db,
            table_name=shredded_failed_table,
        )

    elif failure == "unshredded":

        # Writing raw record to error location on s3
        schema_version_config = param_config["table_schema_version"][env]
        schema_version_config = json.dumps(schema_version_config)
        logger.info(
            f"Writing unshredded failed records to unshredded_failed table for batchid {batch_id}"
        )

        # raw_df = raw_df.withColumn("table_name", f.lit("all_tables").cast("string"))
        raw_df = raw_df.withColumn(
            "schema_version_config", f.lit(schema_version_config)
        ).withColumn("error", f.lit(error).cast("string"))

        table_data, partition_keys = get_current_schema(
            glue_client=glue_client,
            database_name=audit_db,
            table_name=unshredded_failed_table,
        )
        s3_path = table_data["table_location"]
        raw_df.write.mode("append").format("parquet").partitionBy(partition_keys).save(
            f"{s3_path}/"
        )
        logger.info(
            f"Writing completed for unshredded_failed table  for batchid {batch_id}"
        )
        logger.info("Updating glue catalog of unshredded_failed table")

        partition_location = f"{s3_path}/topic_name={topic_name}/dal_year={dal_year}/dal_month={dal_month}/dal_day={dal_day}/dal_hour={dal_hour}/batch_id={batch_id}"

        partition_values = [
            topic_name,
            dal_year,
            dal_month,
            dal_day,
            dal_hour,
            batch_id,
        ]

        generate_partition_glue(
            table_data=table_data,
            glue_client=glue_client,
            partition_location=partition_location,
            partition_values=partition_values,
            database_name=audit_db,
            table_name=unshredded_failed_table,
        )


# get current table schema for the given database name & table name
def get_current_schema(glue_client, database_name, table_name):
    """_summary_

    Args:
        glue_client (_type_): _description_
        database_name (_type_): _description_
        table_name (_type_): _description_

    Returns:
        _type_: _description_
    """

    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    except Exception as error:
        logger.error("Exception while fetching table info")
        # sys.exit(-1)
    # Parsing table info required to create partitions from table
    table_data = {}
    table_data["input_format"] = response["Table"]["StorageDescriptor"]["InputFormat"]
    table_data["output_format"] = response["Table"]["StorageDescriptor"]["OutputFormat"]
    table_data["table_location"] = response["Table"]["StorageDescriptor"]["Location"]
    table_data["serde_info"] = response["Table"]["StorageDescriptor"]["SerdeInfo"]
    table_data["partition_keys"] = response["Table"]["PartitionKeys"]
    partition_keys = []
    for i in table_data["partition_keys"]:
        partition_keys.append(i["Name"])

    return table_data, partition_keys


def generate_partition_glue(
    table_data,
    partition_location,
    partition_values,
    glue_client,
    database_name,
    table_name,
):
    """_summary_

    Args:
        table_data (_type_): _description_
        part_location (_type_): _description_
        glue_client (_type_): _description_
        database_name (_type_): _description_
        table_name (_type_): _description_
        current_time (_type_): _description_
    """
    input_list = []  # Initializing empty list

    input_dict = {
        "Values": partition_values,
        "StorageDescriptor": {
            "Location": partition_location,
            "InputFormat": table_data["input_format"],
            "OutputFormat": table_data["output_format"],
            "SerdeInfo": table_data["serde_info"],
        },
    }

    input_list.append(input_dict.copy())

    try:
        create_partition_response = glue_client.batch_create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInputList=input_list,
        )
        logger.info(f"Glue partition created successfully for {table_name}")
        logger.info(create_partition_response)
    except Exception as e:
        # Handle exception as per your business requirements
        logger.error(e)


def NullType_to_string(df):
    my_schema = list(df.schema)
    null_cols = []
    # # iterate over schema list to filter for NullType columns
    for st in my_schema:
        if str(st.dataType) == "NullType":
            null_cols.append(st)

    for ncol in null_cols:
        mycolname = str(ncol.name)
        df = df.withColumn(mycolname, f.lit(None).cast(StringType()))

    return df


def get_masking_rules_csv(table, bucket, key, s3_client, notifier=None):

    try:
        columns_map = {}
        function_map = {}
        mask_tables = []
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response["Body"].read().decode("utf-8")

        lines = data.splitlines()

        for line in lines:
            single = line.strip('"').split(",")
            if single[6] == "ARCHIVE" and single[4] != "N" and single[0] == table:
                columns_map[single[1].lower()] = single[8].lower()
                function_map[single[1].lower()] = single[7]
                mask_tables.append(single[0])
        return columns_map, function_map, list(set(mask_tables))
    except Exception as e:
        logger.error("Failed While fetching masking rules file")
        logger.error(f"Failed due to Error : {e}")
        send_email_notification(1, notifier, e)
        sys.exit()


def _date_(dob, shredding_timestamp):
    """_summary_

    Args:
        dob (_type_): _description_
        shredding_timestamp (_type_): _description_

    Raises:
        e: _description_
        Exception: _description_

    Returns:
        _type_: _description_
    """

    if dob in ["", "NaT", None]:
        return None
    shredding_timestamp = shredding_timestamp.replace("T", " ")
    dob = dob.replace("T", " ")
    entry_time = datetime.datetime.strptime(shredding_timestamp, "%Y-%m-%d %H:%M:%S")
    dob_date = datetime.datetime.strptime(
        dob, "%Y-%m-%d %H:%M:%S"
    )  #'1987-02-05 00:00:00' , 2020-07-20T07:19:46
    age = relativedelta(entry_time, dob_date).years

    try:
        pseudo_dob = datetime.datetime(
            entry_time.year - age, entry_time.month, entry_time.day
        ).replace(tzinfo=None)
        return pseudo_dob

    except ValueError as e:
        if str(e) == "day is out of range for month":
            if entry_time.month == 2 and entry_time.day == 29:
                pseudo_dob = datetime.datetime(entry_time.year - age, 3, 1).replace(
                    tzinfo=None
                )
                return pseudo_dob
            else:
                raise e
        else:
            raise Exception("Value error has occured")


def _sha512_(mask_key):
    """_summary_

    Args:
        mask_key (_type_): _description_

    Returns:
        _type_: _description_
    """

    try:
        return hashlib.sha512(mask_key.encode()).hexdigest()
    except Exception as e:
        logger.error(
            f"Error has occured on sha512 masking for key {mask_key} and the error is {e}"
        )


class PropagatingThread(InheritableThread):
    def run(self):
        self.exc = None
        try:
            if hasattr(self, "_Thread__target"):
                # Thread uses name mangling prior to Python 3.
                self.ret = self._Thread__target(
                    *self._Thread__args, **self._Thread__kwargs
                )
            else:
                self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super(PropagatingThread, self).join(timeout)
        if self.exc:
            raise self.exc
        return self.ret


def create_message(
    df,
    raw_df,
    table_name,
    batch_id,
    topic_name,
    error_message,
    job_name,
    job_run_id,
    param_config,
    failure,
    env,
):

    logger.info("Starting message creation")

    date = datetime.datetime.today().date().strftime("%Y-%m-%d")
    subject = f""" Shredding job Error detail on {topic_name} on date {date}"""
    ### Truncating the error message to avoid size overflow error
    error_message = str(error_message)[0:2000]
    if failure == "shredded":

        logger.info("Getting countes")
        count_of_quotes = (
            df.withColumn("correlationid", f.col("header.correlationid"))
            .select("correlationid")
            .distinct()
            .count()
        )

        message = f"""
                ##===============================================## 
                ## {job_name} Job Excecution  and Job run id is {job_run_id}
                ##===============================================##

                    "Status" : FAILED
                    "Error" : {error_message}
                    "Batch_id" : {batch_id}
                    "Table_name" : {table_name}
                    "Count of quotes " : {count_of_quotes}
                    "Kafka topic name" : {topic_name}
                ##################################################    
                ##   Note: Find More Information in Log File    ##
                ##################################################
                """

    elif failure == "unshredded":
        table_list = param_config["Topic"][env][topic_name]["table_list"]
        table_list = " , ".join(str(i) for i in table_list)
        count_of_quotes = raw_df.count()
        message = f"""
                ##===============================================## 
                ## {job_name} Job Excecution  and Job run id is {job_run_id}
                ##===============================================##

                    "Status" : FAILED
                    "Error" : {error_message}
                    "Batch_id" : {batch_id}
                    "Table_name" : "{table_list}"
                    "Count of quotes " : {count_of_quotes}
                    
                ##################################################    
                ##   Note: Find More Information in Log File    ##
                ##################################################
                """
    return message, subject


def send_email(err_msg, topic_arn, subject, sns_client):
    """_summary_

    Args:
        err_msg (_type_): _description_
        topic_arn (_type_): _description_
        subject (_type_): _description_
        sns_client (_type_): _description_
    """
    try:
        response = sns_client.publish(
            TopicArn=topic_arn, Message=err_msg, Subject=subject
        )
    except Exception as e:
        # error_code = e.response["Error"]["Code"]
        logger.error(f"Unable to send the mail ------ Error is {e}")


def explode_selected(df, nc):
    if type_cols(df.select(nc).dtypes, "array"):
        return df.withColumn(nc, f.explode(f.col(nc)))
    else:
        return df


def flatten_selected(nested_df, column, sep="-"):
    nested_cols = [column]
    flatten_cols = [fc for fc, _ in nested_df.dtypes if fc not in nested_cols]
    for nc in nested_cols:
        for cc in nested_df.select(f"{nc}.*").columns:
            if sep is None:
                flatten_cols.append(f.col(f"{nc}.{cc}").alias(f"{cc}"))
            else:
                flatten_cols.append(f.col(f"{nc}.{cc}").alias(f"{nc}{sep}{cc}"))
    return nested_df.select(flatten_cols)


def window_column_generator(df, table_name, windows_conf=windows_conf):
    """_summary_

    Args:
        df (_type_): _description_
        windows_conf (_type_): _description_
        table_name (_type_): _description_
        topic_name (_type_): _description_

    Returns:
        _type_: _description_
    """

    if table_name == "quote_header":
        return df, True
    else:
        windows_conf = windows_conf[table_name]

        windows_conf = OrderedDict(
            sorted(windows_conf.items(), key=lambda x: getitem(x[1], "heirarchy"))
        )

        for key, value in windows_conf.items():
            col_to_select = value["source"]
            partitionby = [i + "_ref" for i in value["partitionBy"]]
            orderby = value["orderBy"]
            explode_col = col_to_select.replace(".", "-")

            if has_column(df, col_to_select):
                col_to_select = [
                    f.col(col_to_select).alias(col_to_select.replace(".", "-"))
                ]
                df = df.select(df.columns + col_to_select)
                df = explode_selected(df, explode_col)
                df = df.withColumn(key + "_ref", f.expr("uuid()")).withColumn(
                    key, f.monotonically_increasing_id()
                )

                window = Window.partitionBy(partitionby).orderBy(orderby)
                df = df.withColumn(key, f.row_number().over(window))

            else:
                logger.info(
                    f"No data is present for key {key} so table {table_name}  data won't be generated"
                )
                return df, False
    return df, True


def glue_partition_and_s3path(spark_session, db, table_name):
    spark_session.catalog.setCurrentDatabase(db)
    partitions = [
        col.name
        for col in spark_session.catalog.listColumns(table_name)
        if col.isPartition == True
    ]
    s3_path = (
        spark_session.sql(f"desc formatted {db}.{table_name}")
        .filter("col_name=='Location'")
        .collect()[0]
        .data_type
    )
    return partitions, s3_path


def add_hive_partition(
    spark_session,
    database,
    table,
    partition_cols,
    partition_values,
    partition_location=None,
):
    """
    Adds a Hive partition to a table with a custom location.

    Args:
    - database (str): The name of the Hive database.
    - table (str): The name of the Hive table.
    - partition_cols (list): A list of partition column names.
    - partition_values (list): A list of partition values.
    - partition_location (str): The custom location for the partition.
    """
    # Create a SparkSession

    # Set the current database to the specified database
    spark_session.sql(f"USE {database}")

    # Construct the ALTER TABLE command with the ADD PARTITION and LOCATION clauses
    partition_clause = ",".join(
        [f"{col}='{value}'" for col, value in zip(partition_cols, partition_values)]
    )

    add_partition_sql = f"ALTER TABLE {table} ADD PARTITION ({partition_clause})"

    if partition_location is not None:
        add_partition_sql += f"LOCATION '{partition_location}'"

    # Execute the ALTER TABLE command
    spark_session.sql(add_partition_sql)
