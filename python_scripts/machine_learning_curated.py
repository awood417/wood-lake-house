import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1787001125113 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1787001125113")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1787001125813 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1787001125813")

# Script generated for node SQL Query
SqlQuery1855 = '''
select * from MyDataSource a
INNER JOIN myDataSource2 s
  ON a.timeStamp = s.sensorReadingTime;

'''
SQLQuery_node1787001257811 = sparkSqlQuery(glueContext, query = SqlQuery1855, mapping = {"myDataSource":AWSGlueDataCatalog_node1787001125113, "myDataSource2":AWSGlueDataCatalog_node1787001125813}, transformation_ctx = "SQLQuery_node1787001257811")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787001257811, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787001118544", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1787001521041 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787001257811, connection_type="s3", format="json", connection_options={"path": "s3://wood-lake-house/machine_learning_curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787001521041")

job.commit()