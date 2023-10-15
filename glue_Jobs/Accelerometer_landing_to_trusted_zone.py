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
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hb-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1697373755229 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hb-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1697373755229",
)

# Script generated for node Join
Join_node1697373781875 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1697373755229,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1697373781875",
)

# Script generated for node Drop Fields
DropFields_node1697373820532 = DropFields.apply(
    frame=Join_node1697373781875,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1697373820532",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1697373820532,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-hb-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node2",
)

job.commit()
