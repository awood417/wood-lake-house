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
AWSGlueDataCatalog_node1786905133971 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1786905133971")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1786905134773 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1786905134773")

# Script generated for node SQL Query
SqlQuery1450 = '''
SELECT DISTINCT c.*
FROM myDataSource a
JOIN myDataSource2 c
  ON c.email = a.user
'''
SQLQuery_node1786905196821 = sparkSqlQuery(glueContext, query = SqlQuery1450, mapping = {"myDataSource":AWSGlueDataCatalog_node1786905134773, "myDataSource2":AWSGlueDataCatalog_node1786905133971}, transformation_ctx = "SQLQuery_node1786905196821")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1786905196821, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786905089072", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1786905637823 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1786905196821, connection_type="s3", format="json", connection_options={"path": "s3://wood-lake-house/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1786905637823")

job.commit()