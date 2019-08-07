# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
import base64
import array

# COMMAND ----------

# s is a base64 encoded float[] with first element being the magnitude
def Base64ToFloatArray(s):
  arr = array.array('f', base64.b64decode(s))
  return (arr[0], arr[1:])

def cosineSimilarity(s1, s2):
  (m1, v1) = Base64ToFloatArray(s1)
  (m2, v2) = Base64ToFloatArray(s2)
  if (m1 == 0) or (m2 == 0):
    return 0
  else :
    return sum(x*y for x,y in zip(v1, v2))/(m1 * m2)

# Register udf functions so that it could be used in dataframe
#
# Perform same computation as cosineSimilarity()
#
@F.udf("float")
def udfCosineSimilarity(s1, s2):
  return cosineSimilarity(s1, s2)

# COMMAND ----------

# MAGIC %md **NetworkSimilarity** class to compute Network Similarity

# COMMAND ----------

#   Parameters:
#     resource: resource stream path
#     container: container name in Azure Storage (AS) account
#     account: Azure Storage (AS) account
#     sas: complete 'Blob service SAS URL' of the shared access signature (sas) for the container
#     key: access key for the container, if sas is specified, key is ignored
#
#   Note:
#     resource does not have header
#     you need to provide value for either sas or key
#
class NetworkSimilarity(AzureStorageAccess):
  # constructor
  def __init__(self, resource, container, account, sas='', key=''):
    AzureStorageAccess.__init__(self, container, account, sas, key)
    schema = StructType()
    schema.add(StructField('EntityId', LongType(), False))
    schema.add(StructField('EntityType', StringType(), False))
    schema.add(StructField('Data', StringType(), False))
    self.df = spark.read.format('csv').options(header='false', delimiter='\t').schema(schema).load(self.getFullpath(resource))

  def getDataframe(self):
    return self.df
  
  def raiseErrorIfNotFound(self, row, e):
    if row is None:
      raise KeyError('entity ' + str(e) + ' not found')

  def getSimilarity(self, e1, e2):
    df = self.df
    row1 = df.where(df.EntityId == e1).first()
    self.raiseErrorIfNotFound(row1, e1)
    row2 = df.where(df.EntityId == e2).first()
    self.raiseErrorIfNotFound(row2, e2)
    return cosineSimilarity(row1.Data, row2.Data)

  def getTopEntities(self, e, targetType = '', maxCount = 20, minScore = 0.0):
    df1 = self.df
    row1 = df1.where(df1.EntityId == e).first()
    self.raiseErrorIfNotFound(row1, e)

    if targetType == '':
      df2 = df1.where(df1.EntityId != e)
    else :
      df2 = df1.where((df1.EntityId != e) & (df1.EntityType == targetType))

    df3 = df2.select(df2.EntityId, df2.EntityType, udfCosineSimilarity(F.lit(row1.Data), df2.Data).alias('Score'))
    return df3.where(df3.Score >= minScore).orderBy(df3.Score.desc()).limit(maxCount)
