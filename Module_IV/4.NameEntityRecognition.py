# Databricks notebook source
# MAGIC %md # Module IV. Name Entity Recognition

# COMMAND ----------

import requests
import json

__ner_api = "https://nernips.azurewebsites.net/api"

def ner (text, api=__ner_api):
    resp = requests.post(__ner_api, json=text)
    assert resp.status_code == 200
    respj = json.loads(resp.content.decode('utf-8'))
    return list(zip(respj['input'].split(), respj['output'].split()))

# COMMAND ----------

text = "A generative adversarial network (GAN) is a class of machine learning systems invented by Ian Goodfellow and his colleagues in 2014.[1] Two neural networks contest with each other in a game (in the sense of game theory, often but not always in the form of a zero-sum game). Given a training set, this technique learns to generate new data with the same statistics as the training set. For example, a GAN trained on photographs can generate new photographs that look at least superficially authentic to human observers, having many realistic characteristics. Though originally proposed as a form of generative model for unsupervised learning, GANs have also proven useful for semi-supervised learning,[2] fully supervised learning,[3] and reinforcement learning.[4] In a 2016 seminar, Yann LeCun described GANs as \"the coolest idea in machine learning in the last twenty years\"!"
ner(text)

# COMMAND ----------


