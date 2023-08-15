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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="makbul",
    table_name="step_trainer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1691871036498 = glueContext.create_dynamic_frame.from_catalog(
    database="makbul",
    table_name="accelerometer_trusted",
    transformation_ctx="AmazonS3_node1691871036498",
)

# Script generated for node Join
Join_node1691871044939 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1691871036498,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1691871044939",
)

# Script generated for node Drop Fields
DropFields_node1691871121303 = DropFields.apply(
    frame=Join_node1691871044939,
    paths=["user"],
    transformation_ctx="DropFields_node1691871121303",
)

# Script generated for node Amazon S3
AmazonS3_node1691871134953 = glueContext.getSink(
    path="s3://makbul-unique/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1691871134953",
)
AmazonS3_node1691871134953.setCatalogInfo(
    catalogDatabase="makbul", catalogTableName="machine_learning_curated"
)
AmazonS3_node1691871134953.setFormat("json")
AmazonS3_node1691871134953.writeFrame(DropFields_node1691871121303)
job.commit()

