# Databricks notebook source
# MAGIC %run "./PySparkMagClass"

# COMMAND ----------

# MAGIC %run "./PySparkNetworkSimilarityClass"

# COMMAND ----------

MagAccount = 'kdd2019magstore'
MagContainer = 'mag-2019-06-07'
MagSAS = '' # Enter the shared access signature of MAG Container
ResourcePath = 'ns/AffiliationCopaper.tsv'

# COMMAND ----------

ns = NetworkSimilarity(resource=ResourcePath, container=MagContainer, account=MagAccount, sas=MagSAS)
df = ns.getDataframe()
display(df)

# COMMAND ----------

print(ns.getSimilarity(157347489, 78545622))

# COMMAND ----------

top = ns.getTopEntities(157347489)
display(top)
