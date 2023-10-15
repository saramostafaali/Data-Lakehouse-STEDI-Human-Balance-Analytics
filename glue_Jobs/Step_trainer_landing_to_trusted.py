import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3_step_trainer_landing
S3_step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-hb-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3_step_trainer_landing_node1",
)

# Script generated for node customers_curated
customers_curated_node1686616036920 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="customers_curated",
        transformation_ctx="customers_curated_node1686616036920",
    )
)

# Script generated for node Join
Join_node1686616088440 = Join.apply(
    frame1=S3_step_trainer_landing_node1,
    frame2=customers_curated_node1686616036920,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1686616088440",
)

# Script generated for node Drop Fields
DropFields_node1686616191839 = DropFields.apply(
    frame=Join_node1686616088440,
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
    transformation_ctx="DropFields_node1686616191839",
)

# Script generated for node S3_step_trainer_trusted
S3_step_trainer_trusted_node1686616213247 = (
    glueContext.write_dynamic_frame.from_options(
        frame=DropFields_node1686616191839,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://stedi-hb-lakehouse/step_trainer/trusted/",
            "partitionKeys": [],
        },
        transformation_ctx="S3_step_trainer_trusted_node1686616213247",
    )
)

job.commit()
