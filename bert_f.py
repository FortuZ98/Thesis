import pandas as pd
import pickle
from bertopic import BERTopic

data=pd.read_csv('data_f.csv') #use your path

with open('doc_embedding_f.pickle', 'rb') as pkl: #use your path
    embeddings = pickle.load(pkl)

topic_model = BERTopic(min_topic_size=100, language="multilingual", nr_topics="auto", calculate_probabilities=True, diversity=0.7)
topics, probs = topic_model.fit_transform(data.message_text, embeddings)

data['topic']=topics

data.to_csv('mapping_min_final.csv', index=False)
topic_model.save('topic_model_final')