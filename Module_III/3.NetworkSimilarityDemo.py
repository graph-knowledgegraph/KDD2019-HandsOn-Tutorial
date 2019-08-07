# Databricks notebook source
# MAGIC %run "./TutorialClasses"

# COMMAND ----------

# Azure Storage account name
MagAccount = 'kdd2019magstore'

# MAG container name
MagContainer = 'mag-2019-06-07'

# Shared access signature of MAG Container
MagSAS = ''

# Network Similarity Resource path
ResourcePath = 'ns/AffiliationCopaper.tsv'

# COMMAND ----------

mag = MicrosoftAcademicGraph(container=MagContainer, account=MagAccount, sas=MagSAS)
Affiliations = mag.getDataframe('Affiliations')
display(Affiliations)

# COMMAND ----------

ns = NetworkSimilarity(resource=ResourcePath, container=MagContainer, account=MagAccount, sas=MagSAS)
df = ns.getDataframe()
display(df)

# COMMAND ----------

print(ns.getSimilarity(157347489, 78545622))

# COMMAND ----------

aff = Affiliations.where(Affiliations.AffiliationId == 157347489)
display(aff)

# COMMAND ----------

topEntities = ns.getTopEntities(157347489)
display(topEntities)

# COMMAND ----------

top = topEntities.join(Affiliations, topEntities.EntityId == Affiliations.AffiliationId, 'inner') \
    .select(Affiliations.AffiliationId, Affiliations.DisplayName, topEntities.EntityType, topEntities.Score)
display(top)
