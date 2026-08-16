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
AWSGlueDataCatalog_node1786903715802 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1786903715802")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1786903714686 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1786903714686")

# Script generated for node SQL Query
SqlQuery1408 = '''
SELECT DISTINCT a.*
FROM myDataSource2 a
JOIN myDataSource c
  ON a.user = c.email

'''
SQLQuery_node1786903848518 = sparkSqlQuery(glueContext, query = SqlQuery1408, mapping = {"myDataSource":AWSGlueDataCatalog_node1786903715802, "myDataSource2":AWSGlueDataCatalog_node1786903714686}, transformation_ctx = "SQLQuery_node1786903848518")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1786903848518, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786904571705", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1786904636652 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1786903848518, connection_type="s3", format="json", connection_options={"path": "s3://wood-lake-house/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1786904636652")

job.commit()