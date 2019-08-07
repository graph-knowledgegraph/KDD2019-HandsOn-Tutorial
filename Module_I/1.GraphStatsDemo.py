# Databricks notebook source
# MAGIC %run "./TutorialClasses"

# COMMAND ----------

# Azure Storage account name
MagAccount = 'kdd2019magstore'

# MAG container name
MagContainer = 'mag-2019-06-07'

# Shared access signature of MAG Container
MagSAS = ''

# COMMAND ----------

mag = MicrosoftAcademicGraph(container=MagContainer, account=MagAccount, sas=MagSAS)

# COMMAND ----------

Affiliations = mag.getDataframe('Affiliations')
display(Affiliations)

# COMMAND ----------

Authors = mag.getDataframe('Authors')
ConferenceInstances = mag.getDataframe('ConferenceInstances')
ConferenceSeries = mag.getDataframe('ConferenceSeries')
EntityRelatedEntities = mag.getDataframe('EntityRelatedEntities')
FieldsOfStudy = mag.getDataframe('FieldsOfStudy')
FieldOfStudyChildren = mag.getDataframe('FieldOfStudyChildren')
Journals = mag.getDataframe('Journals')
Papers = mag.getDataframe('Papers')
PaperAbstractsInvertedIndex = mag.getDataframe('PaperAbstractsInvertedIndex')
PaperAuthorAffiliations = mag.getDataframe('PaperAuthorAffiliations')
PaperCitationContexts = mag.getDataframe('PaperCitationContexts')
PaperFieldsOfStudy = mag.getDataframe('PaperFieldsOfStudy')
PaperReferences = mag.getDataframe('PaperReferences')
PaperUrls = mag.getDataframe('PaperUrls')
PaperRecommendations = mag.getDataframe('PaperRecommendations')
PaperResources = mag.getDataframe('PaperResources')
RelatedFieldOfStudy = mag.getDataframe('RelatedFieldOfStudy')

# COMMAND ----------

formatStr = '{:<16} | {:>8}'
print(formatStr.format('Table', 'Count'))
print('-----------------+---------')
print(formatStr.format('Papers', Papers.count()))
print(formatStr.format('Authors', Authors.count()))
print(formatStr.format('FieldsOfStudy', FieldsOfStudy.count()))
print(formatStr.format('ConferenceSeries', ConferenceSeries.count()))
print(formatStr.format('Journals', Journals.count()))
print(formatStr.format('Affiliations', Affiliations.count()))

# COMMAND ----------

formatStr = '{:<27} | {:>8}'
print(formatStr.format('Table', 'Count'))
print('----------------------------+---------')
print(formatStr.format('ConferenceInstances', ConferenceInstances.count()))
print(formatStr.format('EntityRelatedEntities', EntityRelatedEntities.count()))
print(formatStr.format('FieldOfStudyChildren', FieldOfStudyChildren.count()))
print(formatStr.format('PaperAbstractsInvertedIndex', PaperAbstractsInvertedIndex.count()))
print(formatStr.format('PaperAuthorAffiliations', PaperAuthorAffiliations.count()))
print(formatStr.format('PaperCitationContexts', PaperCitationContexts.count()))
print(formatStr.format('PaperFieldsOfStudy', PaperFieldsOfStudy.count()))
print(formatStr.format('PaperReferences', PaperReferences.count()))
print(formatStr.format('PaperUrls', PaperUrls.count()))
print(formatStr.format('PaperRecommendations', PaperRecommendations.count()))
print(formatStr.format('PaperResources', PaperResources.count()))
print(formatStr.format('RelatedFieldOfStudy', RelatedFieldOfStudy.count()))
