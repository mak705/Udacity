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

# Script generated for node accelerometer_landing_trusted
accelerometer_landing_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="makbul",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_trusted_node1",
)

# Script generated for node customer_landing_trusted
customer_landing_trusted_node1691865631975 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="makbul",
        table_name="customer_landing_trusted",
        transformation_ctx="customer_landing_trusted_node1691865631975",
    )
)

# Script generated for node Join
Join_node1691865688976 = Join.apply(
    frame1=accelerometer_landing_trusted_node1,
    frame2=customer_landing_trusted_node1691865631975,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1691865688976",
)

# Script generated for node Drop Fields
DropFields_node1691866193488 = DropFields.apply(
    frame=Join_node1691865688976,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1691866193488",
)

# Script generated for node Amazon S3
AmazonS3_node1691866259063 = glueContext.getSink(
    path="s3://makbul-unique/customer_landing_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1691866259063",
)
AmazonS3_node1691866259063.setCatalogInfo(
    catalogDatabase="makbul", catalogTableName="customer_curated"
)
AmazonS3_node1691866259063.setFormat("json")
AmazonS3_node1691866259063.writeFrame(DropFields_node1691866193488)
job.commit()

