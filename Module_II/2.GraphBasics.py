# Databricks notebook source
# MAGIC %md # Module II. Graph Basics

# COMMAND ----------

# MAGIC %run "./TutorialClasses"

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

MagAccount = 'kdd2019magstore'
MagContainer = 'mag-2019-06-07'
MagSAS = '?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&se=2019-08-10T02:22:11Z&st=2019-07-31T18:22:11Z&spr=https&sig=m8XIxbDhk3ZOBt5ceVYIodw3k0JhbXodUBZDxXOThcs%3D'

# COMMAND ----------

mag = MicrosoftAcademicGraph(container=MagContainer, account=MagAccount, sas=MagSAS)

# COMMAND ----------

# DBTITLE 1,Task 1. Degree Centrality
# Construct author collaboration graph
# Input:  the author list of a paper
# Output: all possible combination of author pairs
# Note that (a1, a2) is the same as (a2, a1)

# paperAuthor
# ======================
# | PaperId | AuthorId |
# ======================
# | p1      | a1       |
# | p1      | a2       |
# | p1      | a3       |
# ======================
paperAuthor = mag.getDataframe('PaperAuthorAffiliations').select('PaperId', 'AuthorId')
paperAuthor1 = paperAuthor.select('PaperId', 'AuthorId').withColumnRenamed('AuthorId', 'Author1')
paperAuthor2 = paperAuthor.select('PaperId', 'AuthorId').withColumnRenamed('AuthorId', 'Author2')

# collaborationGraph
# ===============================
# | PaperId | Author1 | Author2 |
# ===============================
# | p1      | a2      | a1      |
# | p1      | a3      | a1      |
# | p1      | a3      | a2      |
# ===============================
collaborationGraph = paperAuthor1.join(paperAuthor2, paperAuthor1.PaperId == paperAuthor2.PaperId, 'inner').where(paperAuthor1.Author1 > paperAuthor2.Author2).drop(paperAuthor1.PaperId).drop(paperAuthor2.PaperId)
display(collaborationGraph)

# COMMAND ----------

# Compute node degree
# Input:  author collaboration graph
# Output: number of distinct coauthors for each author

# coauthorCount1
# ====================
# | Author1 | Count1 |
# ====================
# | a2      | 1      |
# | a3      | 2      |
# ====================
coauthorCount1 = collaborationGraph.groupBy('Author1').agg(countDistinct('Author2').alias('Count1'))

# coauthorCount2
# ====================
# | Author2 | Count2 |
# ====================
# | a1      | 2      |
# | a2      | 1      |
# ====================
coauthorCount2 = collaborationGraph.groupBy('Author2').agg(countDistinct('Author1').alias('Count2'))

# coauthorCount
# =======================================
# | Author1 | Count1 | Author2 | Count2 |
# =======================================
# | null    | 0      | a1      | 2      |
# | a2      | 1      | a2      | 1      |
# | a3      | 2      | null    | 0      |
# =======================================
coauthorCount = coauthorCount1.join(coauthorCount2, coauthorCount1.Author1 == coauthorCount2.Author2, 'outer')
coauthorCount = coauthorCount.fillna(0, 'Count1').fillna(0, 'Count2')

# coauthorCount
# ================================================
# | Author1 | Count1 | Author2 | Count2 | Degree |
# ================================================
# | null    | 0      | a1      | 2      | 2      |
# | a2      | 1      | a2      | 1      | 2      |
# | a3      | 2      | null    | 0      | 2      |
# ================================================
coauthorCount = coauthorCount.withColumn('Degree', col('Count1') + col('Count2'))
display(coauthorCount)

# COMMAND ----------

# Plot node degree distribution
# x-axis: node degree
# y-axis: number of nodes
nodeDegreeDistribution = coauthorCount.groupBy('Degree').count()
df = nodeDegreeDistribution.orderBy('Degree').toPandas()
plt.clf()
x = df['Degree']
y = df['count']
plt.plot(x, y, '.')
plt.xlabel('Node Degree')
plt.ylabel('Number of Node')
plt.xscale('log')
plt.yscale('log')
display(plt.show())

# COMMAND ----------

# DBTITLE 1,Task 2. Temporal Dynamics
# Compute number of papers for each year

# paper
# ==================
# | PaperId | Year |
# ==================
# | p1      | y1   |
# | p2      | y1   |
# | p3      | y2   |
# ==================
paper = mag.getDataframe('Papers')

# numPaperByYear
# ================
# | Year | count |
# ================
# | y1   | 2     |
# | y2   | 1     |
# ================
numPaperByYear = paper.groupBy('Year').count()
display(numPaperByYear)

# COMMAND ----------

# Compute number of AI-related papers for each year

# paperFos
# ============================
# | PaperId | FieldOfStudyId |
# ============================
# | p1      | f1             |
# | p1      | f2             |
# | p2      | f3             |
# | p3      | f1             |
# ============================
paperFos = mag.getDataframe('PaperFieldsOfStudy')

# ai
# ============================================
# | FieldOfStudyId | NormalizedName          |
# ============================================
# | f1             | artificial intelligence |
# ============================================
ai = mag.getDataframe('FieldsOfStudy').filter(col('NormalizedName') == 'artificial intelligence')

# aiPaperId
# ============================
# | PaperId | FieldOfStudyId |
# ============================
# | p1      | f1             |
# | p3      | f1             |
# ============================
aiPaperId = paperFos.join(ai, paperFos.FieldOfStudyId == ai.FieldOfStudyId, 'inner').select('PaperId')

# aiPaper
# ==================
# | PaperId | Year |
# ==================
# | p1      | y1   |
# | p3      | y2   |
# ==================
aiPaper = paper.join(aiPaperId, paper.PaperId == aiPaperId.PaperId, 'inner')

# numAiPaperByYear
# ================
# | Year | count |
# ================
# | y1   | 1     |
# | y2   | 1     |
# ================
numAiPaperByYear = aiPaper.groupBy('Year').count()
display(numAiPaperByYear)

# COMMAND ----------

# Plot the distribution of all papers and AI-related papers over the past 50 years
paperDf = numPaperByYear.orderBy('Year').toPandas()
aiPaperDf = numAiPaperByYear.orderBy('Year').toPandas()
plt.clf()
x = paperDf['Year']
y = paperDf['count']
plt.plot(x,y)
x = aiPaperDf['Year']
y = aiPaperDf['count']
plt.plot(x,y)

plt.xlim(1970, 2018)
plt.xlabel('Year')
plt.ylabel('Number of Paper')
plt.yscale('log')
display(plt.show())
