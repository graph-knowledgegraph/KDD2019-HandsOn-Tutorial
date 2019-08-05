# Databricks notebook source
# MAGIC %run "./PySparkMagClass"

# COMMAND ----------

MagAccount = 'kdd2019magstore'
MagContainer = 'mag-2019-06-07'
MagSAS = '' # Enter the shared access signature of MAG Container

# COMMAND ----------

mag = MicrosoftAcademicGraph(container=MagContainer, account=MagAccount, sas=MagSAS)

# COMMAND ----------

Affiliations = mag.getDataframe('Affiliations')
print('Affiliations count:', Affiliations.count())

# COMMAND ----------

Authors = mag.getDataframe('Authors')
print('Authors count:', Authors.count())

# COMMAND ----------

ConferenceInstances = mag.getDataframe('ConferenceInstances')
print('ConferenceInstances count:', ConferenceInstances.count())

# COMMAND ----------

ConferenceSeries = mag.getDataframe('ConferenceSeries')
print('ConferenceSeries count:', ConferenceSeries.count())

# COMMAND ----------

EntityRelatedEntities = mag.getDataframe('EntityRelatedEntities')
print('EntityRelatedEntities count:', EntityRelatedEntities.count())

# COMMAND ----------

FieldsOfStudy = mag.getDataframe('FieldsOfStudy')
print('FieldsOfStudy count:', FieldsOfStudy.count())

# COMMAND ----------

FieldOfStudyChildren = mag.getDataframe('FieldOfStudyChildren')
print('FieldOfStudyChildren count:', FieldOfStudyChildren.count())

# COMMAND ----------

Journals = mag.getDataframe('Journals')
print('Journals count:', Journals.count())

# COMMAND ----------

Papers = mag.getDataframe('Papers')
print('Papers count:', Papers.count())

# COMMAND ----------

PaperAbstractsInvertedIndex = mag.getDataframe('PaperAbstractsInvertedIndex')
print('PaperAbstractsInvertedIndex count:', PaperAbstractsInvertedIndex.count())

# COMMAND ----------

PaperAuthorAffiliations = mag.getDataframe('PaperAuthorAffiliations')
print('PaperAuthorAffiliations count:', PaperAuthorAffiliations.count())

# COMMAND ----------

PaperCitationContexts = mag.getDataframe('PaperCitationContexts')
print('PaperCitationContexts count:', PaperCitationContexts.count())

# COMMAND ----------

PaperFieldsOfStudy = mag.getDataframe('PaperFieldsOfStudy')
print('PaperFieldsOfStudy count:', PaperFieldsOfStudy.count())

# COMMAND ----------

PaperReferences = mag.getDataframe('PaperReferences')
print('PaperReferences count:', PaperReferences.count())

# COMMAND ----------

PaperUrls = mag.getDataframe('PaperUrls')
print('PaperUrls count:', PaperUrls.count())

# COMMAND ----------

PaperRecommendations = mag.getDataframe('PaperRecommendations')
print('PaperRecommendations count:', PaperRecommendations.count())

# COMMAND ----------

PaperResources = mag.getDataframe('PaperResources')
print('PaperResources count:', PaperResources.count())

# COMMAND ----------

RelatedFieldOfStudy = mag.getDataframe('RelatedFieldOfStudy')
print('RelatedFieldOfStudy count:', RelatedFieldOfStudy.count())
