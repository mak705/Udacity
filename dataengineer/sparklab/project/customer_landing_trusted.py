import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

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
        "paths": ["s3://makbul-unique/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Filter
Filter_node1691861450329 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1691861450329",
)

# Script generated for node Amazon S3
AmazonS3_node1691861469540 = glueContext.getSink(
    path="s3://makbul-unique/customer_landing_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1691861469540",
)
AmazonS3_node1691861469540.setCatalogInfo(
    catalogDatabase="makbul", catalogTableName="customer_landing_trusted"
)
AmazonS3_node1691861469540.setFormat("glueparquet")
AmazonS3_node1691861469540.writeFrame(Filter_node1691861450329)
job.commit()

