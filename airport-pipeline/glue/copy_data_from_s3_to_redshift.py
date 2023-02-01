from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="trx",
    table_name="rawlayer_trx",
    transformation_ctx="S3bucket_node1",
)


ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("airport_code", "string", "airport_code", "string"),
        ("airport_name", "string", "airport_name", "string"),
        ("time_label", "string", "time_label", "string"),
        ("time_month", "long", "time_month", "long"),
        ("time_month_name", "string", "time_month_name", "string"),
        ("time_year", "long", "time_year", "long"),
        (
            "statistics_numb_of_delays_carrier",
            "long",
            "statistics_numb_of_delays_carrier",
            "long",
        ),
        (
            "statistics_numb_of_delays_late_aircraft",
            "long",
            "statistics_numb_of_delays_late_aircraft",
            "long",
        ),
        (
            "statistics_numb_of_delays_national_aviation_system",
            "long",
            "statistics_numb_of_delays_national_aviation_system",
            "long",
        ),
        (
            "statistics_numb_of_delays_security",
            "long",
            "statistics_numb_of_delays_security",
            "long",
        ),
        (
            "statistics_numb_of_delays_weather",
            "long",
            "statistics_numb_of_delays_weather",
            "long",
        ),
        (
            "statistics_carriers_names",
            "string",
            "statistics_carriers_names",
            "string",
        ),
        (
            "statistics_carriers_total",
            "long",
            "statistics_carriers_total",
            "long",
        ),
        (
            "statistics_flights_cancelled",
            "long",
            "statistics_flights_cancelled",
            "long",
        ),
        (
            "statistics_flights_delayed",
            "long",
            "statistics_flights_delayed",
            "long",
        ),
        (
            "statistics_flights_diverted",
            "long",
            "statistics_flights_diverted",
            "long",
        ),
        (
            "statistics_flights_on_time",
            "long",
            "statistics_flights_on_time",
            "long",
        ),
        (
            "statistics_flights_total",
            "long",
            "statistics_flights_total",
            "long",
        ),
        (
            "statistics_minutes_delayed_carrier",
            "long",
            "statistics_minutes_delayed_carrier",
            "long",
        ),
        (
            "statistics_minutes_delayed_late_aircraft",
            "long",
            "statistics_minutes_delayed_late_aircraft",
            "long",
        ),
        (
            "statistics_minutes_delayed_national_aviation_system",
            "long",
            "statistics_minutes_delayed_national_aviation_system",
            "long",
        ),
        (
            "statistics_minutes_delayed_security",
            "long",
            "statistics_minutes_delayed_security",
            "long",
        ),
        (
            "statistics_minutes_delayed_total",
            "long",
            "statistics_minutes_delayed_total",
            "long",
        ),
        (
            "statistics_minutes_delayed_weather",
            "long",
            "statistics_minutes_delayed_weather",
            "long",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)


RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="redshift",
    table_name="dev_public_airlines",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
