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
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://makbul-unique/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1691867217212 = glueContext.create_dynamic_frame.from_catalog(
    database="makbul",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1691867217212",
)

# Script generated for node Join
Join_node1691867291011 = Join.apply(
    frame1=AWSGlueDataCatalog_node1691867217212,
    frame2=S3bucket_node1,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1691867291011",
)

# Script generated for node Drop Fields
DropFields_node1691867330794 = DropFields.apply(
    frame=Join_node1691867291011,
    paths=[
        "serialnumber",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1691867330794",
)

# Script generated for node Amazon S3
AmazonS3_node1691867437328 = glueContext.getSink(
    path="s3://makbul-unique/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1691867437328",
)
AmazonS3_node1691867437328.setCatalogInfo(
    catalogDatabase="makbul", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1691867437328.setFormat("json")
AmazonS3_node1691867437328.writeFrame(DropFields_node1691867330794)
job.commit()

