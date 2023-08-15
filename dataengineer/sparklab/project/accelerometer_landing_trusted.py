import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing_trusted
customer_landing_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="makbul",
    table_name="customer_landing_trusted",
    transformation_ctx="customer_landing_trusted_node1",
)

# Script generated for node accelerometer_landing_trusted
accelerometer_landing_trusted_node1691864513162 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="makbul",
        table_name="accelerometer_landing",
        transformation_ctx="accelerometer_landing_trusted_node1691864513162",
    )
)

# Script generated for node Join
Join_node1691864532364 = Join.apply(
    frame1=accelerometer_landing_trusted_node1691864513162,
    frame2=customer_landing_trusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1691864532364",
)

# Script generated for node Drop Fields
DropFields_node1691864675260 = DropFields.apply(
    frame=Join_node1691864532364,
    paths=[
        "phone",
        "sharewithfriendsasofdate",
        "email",
        "customername",
        "sharewithresearchasofdate",
        "registrationdate",
        "sharewithpublicasofdate",
        "serialnumber",
        "lastupdatedate",
        "birthday",
    ],
    transformation_ctx="DropFields_node1691864675260",
)

# Script generated for node Amazon S3
AmazonS3_node1691864599709 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1691864532364,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dend-lake-house/accelerometer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1691864599709",
)

job.commit()

