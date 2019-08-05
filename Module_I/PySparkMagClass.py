# Databricks notebook source
#
# AzureStorageAccess class to access Azure Storage streams
#
#   Parameters:
#     container: container name in Azure Storage (AS) account
#     account: Azure Storage (AS) account name
#     sas: complete 'Blob service SAS URL' of the shared access signature (sas) for the container
#     key: access key for the container, if sas is specified, key is ignored
#
#   Note:
#     you need to provide value for either sas or key
#
class AzureStorageAccess:
  # constructor
  def __init__(self, container, account, sas='', key=''):

    if container == '':
      raise ValueError('container should not be empty')
    
    if account == '':
      raise ValueError('account should not be empty')
    
    if sas == '' and key == '' :
      raise ValueError('provide value for either sas or key')
    
    self.container = container
    self.account = account

    # Set up an account access key or a SAS for the container
    # Once an account access key or a SAS is set up in your notebook, you can use standard Spark and Databricks APIs to read from the storage account
    # Use SAS first then account access key
    if sas != '':
      spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (container, account), sas)
    else :
      spark.conf.set('fs.azure.account.key.%s.blob.core.windows.net' % account, key)

  def getFullpath(self, path):
    return 'wasbs://%s@%s.blob.core.windows.net/%s' % (self.container, self.account, path)

# COMMAND ----------

# MAGIC %md **MicrosoftAcademicGraph** class to access MAG streams

# COMMAND ----------

#
# MicrosoftAcademicGraph class to access MAG streams
#
#   Parameters:
#     container: container name in Azure Storage (AS) account for the MAG dataset. Usually in forms of mag-yyyy-mm-dd
#     account: Azure Storage (AS) account containing MAG dataset
#     sas: complete 'Blob service SAS URL' of the shared access signature (sas) for the container
#     key: access key for the container, if sas is specified, key is ignored
#
#   Note:
#     you need to provide value for either sas or key
#     MAG streams do not have header
#
from pyspark.sql.types import *

class MicrosoftAcademicGraph(AzureStorageAccess):
  # constructor
  def __init__(self, container, account, sas='', key=''):
    AzureStorageAccess.__init__(self, container, account, sas, key) 

  # return stream path
  def getFullpath(self, streamName):
    return AzureStorageAccess.getFullpath(self, self.streams[streamName][0])

  # return stream header
  def getHeader(self, streamName):
    return self.streams[streamName][1]

  datatypedict = {
    'int' : IntegerType(),
    'uint' : IntegerType(),
    'long' : LongType(),
    'ulong' : LongType(),
    'float' : FloatType(),
    'string' : StringType(),
    'DateTime' : DateType(),
  }

  # return stream schema
  def getSchema(self, streamName):
    schema = StructType()
    for field in self.streams[streamName][1]:
      fieldname, fieldtype = field.split(':')
      nullable = fieldtype.endswith('?')
      if nullable:
        fieldtype = fieldtype[:-1]
      schema.add(StructField(fieldname, self.datatypedict[fieldtype], nullable))
    return schema

  # return stream dataframe
  def getDataframe(self, streamName):
    return spark.read.format('csv').options(header='false', delimiter='\t').schema(self.getSchema(streamName)).load(self.getFullpath(streamName))

  # define stream dictionary
  streams = {
    'Affiliations' : ('mag/Affiliations.txt', ['AffiliationId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'GridId:string', 'OfficialPage:string', 'WikiPage:string', 'PaperCount:long', 'CitationCount:long', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    'Authors' : ('mag/Authors.txt', ['AuthorId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'LastKnownAffiliationId:long?', 'PaperCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'ConferenceInstances' : ('mag/ConferenceInstances.txt', ['ConferenceInstanceId:long', 'NormalizedName:string', 'DisplayName:string', 'ConferenceSeriesId:long', 'Location:string', 'OfficialUrl:string', 'StartDate:DateTime?', 'EndDate:DateTime?', 'AbstractRegistrationDate:DateTime?', 'SubmissionDeadlineDate:DateTime?', 'NotificationDueDate:DateTime?', 'FinalVersionDueDate:DateTime?', 'PaperCount:long', 'CitationCount:long', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    'ConferenceSeries' : ('mag/ConferenceSeries.txt', ['ConferenceSeriesId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'PaperCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'EntityRelatedEntities' : ('advanced/EntityRelatedEntities.txt', ['EntityId:long', 'EntityType:string', 'RelatedEntityId:long', 'RelatedEntityType:string', 'RelatedType:int', 'Score:float']),
    'FieldOfStudyChildren' : ('advanced/FieldOfStudyChildren.txt', ['FieldOfStudyId:long', 'ChildFieldOfStudyId:long']),
    'FieldsOfStudy' : ('advanced/FieldsOfStudy.txt', ['FieldOfStudyId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'MainType:string', 'Level:int', 'PaperCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'Journals' : ('mag/Journals.txt', ['JournalId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'Issn:string', 'Publisher:string', 'Webpage:string', 'PaperCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'PaperAbstractsInvertedIndex' : ('nlp/PaperAbstractsInvertedIndex.txt', ['PaperId:long', 'IndexedAbstract:string']),
    'PaperAuthorAffiliations' : ('mag/PaperAuthorAffiliations.txt', ['PaperId:long', 'AuthorId:long', 'AffiliationId:long?', 'AuthorSequenceNumber:uint', 'OriginalAuthor:string', 'OriginalAffiliation:string']),
    'PaperCitationContexts' : ('nlp/PaperCitationContexts.txt', ['PaperId:long', 'PaperReferenceId:long', 'CitationContext:string']),
    'PaperFieldsOfStudy' : ('advanced/PaperFieldsOfStudy.txt', ['PaperId:long', 'FieldOfStudyId:long', 'Score:float']),
    'PaperLanguages' : ('nlp/PaperLanguages.txt', ['PaperId:long', 'LanguageCode:string']),
    'PaperRecommendations' : ('advanced/PaperRecommendations.txt', ['PaperId:long', 'RecommendedPaperId:long', 'Score:float']),
    'PaperReferences' : ('mag/PaperReferences.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperResources' : ('mag/PaperResources.txt', ['PaperId:long', 'ResourceType:int', 'ResourceUrl:string', 'SourceUrl:string', 'RelationshipType:int']),
    'PaperUrls' : ('mag/PaperUrls.txt', ['PaperId:long', 'SourceType:int?', 'SourceUrl:string']),
    'Papers' : ('mag/Papers.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string', 'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?', 'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?', 'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string', 'LastPage:string', 'ReferenceCount:long', 'CitationCount:long', 'EstimatedCitation:long', 'OriginalVenue:string', 'CreatedDate:DateTime']),
    'RelatedFieldOfStudy' : ('advanced/RelatedFieldOfStudy.txt', ['FieldOfStudyId1:long', 'Type1:string', 'FieldOfStudyId2:long', 'Type2:string', 'Rank:float']),
  }

# COMMAND ----------

# MAGIC %md **AzureStorageUtil** class to access Azure Storage streams

# COMMAND ----------

#
# AzureStorageUtil class to access Azure Storage streams
#
#   Parameters:
#     container: container name in Azure Storage (AS) account for input/output streams in PySpark notebook
#     account: Azure Storage (AS) account
#     sas: complete 'Blob service SAS URL' of the shared access signature (sas) for the container
#     key: access key for the container, if sas is specified, key is ignored
#
#   Note:
#     you need to provide value for either sas or key
#     streams contain headers
#
class AzureStorageUtil(AzureStorageAccess):
  # constructor
  def __init__(self, container, account, sas='', key=''):
    AzureStorageAccess.__init__(self, container, account, sas, key) 

  def load(self, path):
    _path = self.getFullpath(path)
    print('laoding from ' + _path)
    return spark.read.format('csv').options(header='true', inferSchema='true').load(_path)

  def save(self, df, path, coalesce=False):
    _path = self.getFullpath(path)
    print('saving to ' + _path)
    if coalesce:
      df.coalesce(1).write.mode('overwrite').format('csv').option('header','true').save(_path)
    else :
      df.write.mode('overwrite').format('csv').option('header','true').save(_path)
