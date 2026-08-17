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
AWSGlueDataCatalog_node1786906124189 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1786906124189")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1786906122722 = glueContext.create_dynamic_frame.from_catalog(database="wood", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1786906122722")

# Script generated for node SQL Query
SqlQuery1425 = '''
SELECT DISTINCT s.*
FROM myDataSource c
JOIN myDataSource2 s
  ON s.serialNumber = c.serialNumber
'''
SQLQuery_node1786906213339 = sparkSqlQuery(glueContext, query = SqlQuery1425, mapping = {"myDataSource":AWSGlueDataCatalog_node1786906124189, "myDataSource2":AWSGlueDataCatalog_node1786906122722}, transformation_ctx = "SQLQuery_node1786906213339")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1786906213339, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1786906829521", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1786906869967 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1786906213339, connection_type="s3", format="json", connection_options={"path": "s3://wood-lake-house/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1786906869967")

job.commit()