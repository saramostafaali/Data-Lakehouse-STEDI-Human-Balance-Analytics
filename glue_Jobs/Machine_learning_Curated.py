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

# Script generated for node stepTrainer trusted
stepTrainertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="stepTrainertrusted_node1",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1677331617859 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1677331617859",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=stepTrainertrusted_node1,
    frame2=Accelerometertrusted_node1677331617859,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1677331721299 = DropFields.apply(
    frame=Join_node2,
    paths=["timeStamp", "user", "serialNumber"],
    transformation_ctx="DropFields_node1677331721299",
)

# Script generated for node S3 machine learning data
S3machinelearningdata_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677331721299,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-hb-lakehouse/step_trainer/ml_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3machinelearningdata_node3",
)

job.commit()
