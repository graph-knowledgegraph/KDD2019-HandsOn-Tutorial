# Databricks notebook source
# MAGIC %md # Paper Recommendations and Content Similarity

# COMMAND ----------

# MAGIC %run "./TutorialClasses"

# COMMAND ----------

# Connect to MAG data store

MagAccount = 'kdd2019magstore'
MagContainer = 'mag-2019-06-07'
MagSAS = '?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&se=2019-08-10T02:22:11Z&st=2019-07-31T18:22:11Z&spr=https&sig=m8XIxbDhk3ZOBt5ceVYIodw3k0JhbXodUBZDxXOThcs%3D'
mag = MicrosoftAcademicGraph(container=MagContainer, account=MagAccount, sas=MagSAS)

# COMMAND ----------

# MAGIC %md ## Content Similarity

# COMMAND ----------

import requests
import json

__ls_api = 'https://ma-languagesimilarity.azurewebsites.net/api/similarity/language/'

def lookup_names_by_ids (ids):
  FieldsOfStudy = mag.getDataframe('FieldsOfStudy')
  idDF = spark.createDataFrame(ids, LongType())
  fosNames = FieldsOfStudy.join(idDF, idDF.value == FieldsOfStudy.FieldOfStudyId, 'inner').select(FieldsOfStudy.FieldOfStudyId, FieldsOfStudy.DisplayName)
  return [(int(row['FieldOfStudyId']), str(row['DisplayName'])) for row in fosNames.collect()]
  
def labelconcepts (text, maxcount=20, minscore=0, api=__ls_api):
  """
  Labels the input text with a list of related concepts.

  Parameters
  ----------
  text : string
      Any arbitrary text string to label.

  maxcount : int (default=20)
      The maximum number of concepts to label the input string with.

  minscore : double (range=[-1, 1], default=0)
      The minimum score cutoff threshold below which concepts will not be returned.

  api : string (default=cloud release endpoint)
      The endpoint you wish to hit for the concept labeling task. Default is the release version of the Microsoft Academic Language Similarity API.

  Returns
  -------
  concept_labels : list of (concept_id, concept_name, score) tuples
      An ordered (descending, by score) list of concepts that the input string was labeled with.
  """
  result = []
  body = {
    "Text": text,
    "MaxCount": maxcount,
    "MinScore": minscore
  }
  endpoint = api + 'labelconcepts'
  resp = requests.post(endpoint, json=body)
  assert resp.status_code == 200, endpoint + " failed with status: " + str(resp.status_code)
  respj = json.loads(resp.content.decode('utf-8'))
  named_ids = lookup_names_by_ids([x['Label'] for x in respj])
  if named_ids:
    result = [(z[0][0], z[0][1], z[1]['Score']) for z in zip(named_ids, respj)]
  return result

def comparetext (text1, text2, api=__ls_api):
  """
  Computes the semantic similarity between two given text strings. Returns a score between [-1, 1].

  Parameters
  ----------
  text1 : string

  text2 : string

  api : string (default=cloud release endpoint)
      The endpoint you wish to hit for the concept labeling task. Default is the release version of the Microsoft Academic Language Similarity API.    
  """
  body = {
    "Text1": text1,
    "Text2": text2
  }
  endpoint = api + 'comparetext'
  resp = requests.post(endpoint, json=body)
  assert resp.status_code == 200, endpoint + " failed with status: " + str(resp.status_code)
  return float(resp.content)
        
def comparelabel (text, concept_id, api=__ls_api):
  """
  Computes the semantic similarity between the given text string and a concept. Returns a score between [-1, 1].

  Parameters
  ----------
  text : string

  id : int
      The id of the concept to compare against the given text.

  api : string (default=cloud release endpoint)
      The endpoint you wish to hit for the concept labeling task. Default is the release version of the Microsoft Academic Language Similarity API.    
  """
  body = {
    "Text": text,
    "Label": concept_id
  }
  endpoint = api + 'comparelabel'
  resp = requests.post(endpoint, json=body)
  assert resp.status_code == 200, endpoint + " failed with status: " + str(resp.status_code)
  return float(resp.content)

# COMMAND ----------

text1 = "Machine learning (ML) is the scientific study of algorithms and statistical models that computer systems use in order to perform a specific task effectively without using explicit instructions, relying on patterns and inference instead. It is seen as a subset of artificial intelligence. Machine learning algorithms build a mathematical model based on sample data, known as \"training data\", in order to make predictions or decisions without being explicitly programmed to perform the task. Machine learning algorithms are used in a wide variety of applications, such as email filtering, and computer vision, where it is infeasible to develop an algorithm of specific instructions for performing the task. Machine learning is closely related to computational statistics, which focuses on making predictions using computers. The study of mathematical optimization delivers methods, theory and application domains to the field of machine learning. Data mining is a field of study within machine learning, and focuses on exploratory data analysis through unsupervised learning. In its application across business problems, machine learning is also referred to as predictive analytics."
text2 = "In computer science, artificial intelligence (AI), sometimes called machine intelligence, is intelligence demonstrated by machines, in contrast to the natural intelligence displayed by humans. Colloquially, the term \"artificial intelligence\" is often used to describe machines (or computers) that mimic \"cognitive\" functions that humans associate with the human mind, such as \"learning\" and \"problem solving\". As machines become increasingly capable, tasks considered to require \"intelligence\" are often removed from the definition of AI, a phenomenon known as the AI effect. A quip in Tesler's Theorem says \"AI is whatever hasn't been done yet.\"For instance, optical character recognition is frequently excluded from things considered to be AI, having become a routine technology. Modern machine capabilities generally classified as AI include successfully understanding human speech, competing at the highest level in strategic game systems (such as chess and Go), autonomously operating cars, intelligent routing in content delivery networks, and military simulations."

# COMMAND ----------

# Compare two strings
comparetext(text1, text2)

# COMMAND ----------

# Label a string
labelconcepts(text1)

# COMMAND ----------

# Compare a string and a concept
comparelabel(text2, 154945302)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paper Recommendation
# MAGIC 
# MAGIC Paper recommendation is done in 2 steps:
# MAGIC 
# MAGIC 1. Using network features. Here we use the co-citations between papers to find related papers.
# MAGIC 2. Using content features. Here we use paper embedings to find similar papers based on cosine similarty.

# COMMAND ----------

# KDD 2016 - XGBoost: A Scalable Tree Boosting System
paperId1 = 2295598076

# NerIPS 2017 - LightGBM: a highly efficient gradient boosting decision tree
paperId2 = 2768348081

# ICLR 2015 - Very Deep Convolutional Networks for Large-Scale Image Recognition
paperId3 = 1686810756

# CVPR 2014 - Rich Feature Hierarchies for Accurate Object Detection and Semantic Segmentation
paperId4 = 2102605133

# COMMAND ----------

paperSim = PaperSimilarity('advanced/PaperEmbeddings.txt', MagContainer, MagAccount, sas=MagSAS)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1. Network Features (Co-citaton) 

# COMMAND ----------

cocitationResultDf = paperSim.getTopEntities(paperId3, method='cocitation')
cocitationResultDf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Content Features (Embeddings) 

# COMMAND ----------

embResultDf = paperSim.getTopEntities(paperId2, method='embedding')
embResultDf.show()

# COMMAND ----------

embResultDf = paperSim.getTopEntities(paperId4, method='embedding')
embResultDf.show()
