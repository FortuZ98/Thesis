import pandas as pd
from collect import PgConn, Database, SyncTelegramClient
import yaml
import numpy as np
import matplotlib.pyplot as plt
from rapidfuzz.distance import Levenshtein
import plotly.express as px
import tldextract
import json
from urlextract import URLExtract


with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)
db = Database(
    "dbname='telegramdb' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)

exluded_channels= db.get_excluded()
exluded_channels=[i[0] for i in exluded_channels]

mappings= db.get_mappings()   # ho gia salvato nel db i pair escludendo canali con pochi mess ecc

bios= db.get_bio()
bio= dict((x, y) for x, y in bios)

titles_usernames= db.get_title_username()
title_username= dict((x, (y,z)) for x, y, z in titles_usernames)

bio_similarities=[]
title_similarities=[]
username_similarities=[]

URLs= db.get_urls(exluded_channels)

urls= dict((x, list(y.split(","))) for x, y in URLs) #url non visibili(nascosti nel testo,in parole,numeri,frasi o link ridotti)
domains=dict()

url_messages= db.get_url_messages(exluded_channels) #con questo ho preso messaggi con url visibili

extractor = URLExtract()

'''
QUESTO PER SALVARE TUTTE GLI URL SU JSON (AGGIUNTO QUELLI VISIBILI AI NON VISIBILI)    #con extractor mi riconosce url dalla struttra tipica, quindi solo se in un messaggio contenente entityurl(link intero visibile) c'è anche uno entitytext che ha forma di link questo viene estratto e contato 2 volte perchè lo avevo già considerato prima, ma i casi sono pochi penso quindi non importa
cont=0
for u in url_messages:
  cont+=1
  urls_list = extractor.find_urls(u[2])
  
 # print(urls_list)
  urls.setdefault(u[0],[])

  urls[u[0]]= urls[u[0]] + urls_list

with open('channel_url_lists2.json', 'w') as fp:     #per salvarlo senza dover runnare per mezzora ogni volta
    json.dump(urls, fp)  
'''  



with open('channel_url_lists2.json', 'r') as fp:     
    channel_url_lists= json.load(fp)

   
'''#save files
res = dict()
res1=dict()
res2=dict()
res3=dict()
res4=dict()
cont=0
for channel_id, url_list in channel_url_lists.items():
  cont+=1 
  l=[]
  for i in url_list:
   if i!='':
    if i[0]==' ':
      i=i[1:]
    ext = tldextract.extract(i)
    l.append((ext.domain+'.'+ext.suffix).lower())   #estraggo il dominio in minuscolo
      
  res[channel_id]=l 
  for i in l:
      res1[i] = l.count(i)  
  my_keys = sorted(res1, key=res1.get, reverse=True)[:20]
  my_keys1 = [k for k, v in res1.items() if v >= np.mean(list(res1.values()))]
  res2[channel_id]=my_keys
  res3[channel_id]=my_keys1
  res1=dict()

with open('res.json', 'w') as fp:     #per salvarlo senza dover runnare per mezzora
    json.dump(res, fp) 
with open('res2.json', 'w') as fp:     
    json.dump(res2, fp) 
with open('res3.json', 'w') as fp:     
    json.dump(res3, fp)'''


def jaccard_similarity(A, B):
    #Find intersection of two sets
    nominator = A.intersection(B)

    #Find union of two sets
    denominator = A.union(B)

    #Take the ratio of sizes
    similarity = len(nominator)/len(denominator)
    
    return similarity

#load files
with open('res.json', 'r') as fp:   
    res=json.load(fp)
with open('res2.json', 'r') as fp:   
    res2=json.load(fp)
with open('res3.json', 'r') as fp:   
    res3=json.load(fp)

'''sempre per capire quanti top prendere, non serve
l=[]
for i in res.values():
  l.append(len(set(i)))
cont=0
cont1=0
cont2=0
cont3=0
cont4=0
cont5=0
for i in l:
 if i<20:
  cont+=1
 else:
  cont5+=1

print(cont,cont1,cont2,cont3,cont4,cont5)
print(np.mean(l))
print(np.min(l))
print(np.max(l))
print(np.median(l))
'''
#computation of similarities

j_sim=[]
top_j_sim=[]
up_j_sim=[]

for m in mappings:  #diversi solo per una frase esce alto per questo normalizzato
      channel_id_verified=m[0]
      channel_id_fake=m[1]
      bio_similarity= Levenshtein.normalized_similarity(bio[channel_id_verified],bio[channel_id_fake])
      title_similarity= Levenshtein.normalized_similarity(title_username[channel_id_verified][0],title_username[channel_id_fake][0])
      username_similarity= Levenshtein.normalized_similarity(title_username[channel_id_verified][1],title_username[channel_id_fake][1])
      bio_similarities.append(bio_similarity)
      title_similarities.append(title_similarity)
      username_similarities.append(username_similarity)
      if str(channel_id_verified) in res.keys() and str(channel_id_fake)  in res.keys():
        all_dom=jaccard_similarity(set(res[str(channel_id_verified)]), set(res[str(channel_id_fake)]))
        j_sim.append(all_dom)
        top_dom= jaccard_similarity(set(res2[str(channel_id_verified)]), set(res2[str(channel_id_fake)]))
        top_j_sim.append(top_dom)
        up_dom=jaccard_similarity(set(res3[str(channel_id_verified)]), set(res3[str(channel_id_fake)]))
        up_j_sim.append(up_dom)
        if all_dom>0.6:
          print(channel_id_verified,channel_id_fake, 'all', all_dom, bio_similarity, title_similarity, username_similarity )
        if top_dom>0.6:
          print(channel_id_verified,channel_id_fake, 'top', top_dom, bio_similarity, title_similarity, username_similarity)
        if up_dom>0.6:
          print(channel_id_verified,channel_id_fake, 'up', up_dom, bio_similarity, title_similarity, username_similarity)
 


 #       j_sim.append(jaccard_similarity(set(res2[str(channel_id_verified)]), set(res2[str(channel_id_fake)])))   per capire se prendere primi 20,15,10 , sta parte si puo togliere
  #      top_j_sim.append(jaccard_similarity(set(res2[str(channel_id_verified)][0:15]), set(res2[str(channel_id_fake)][0:15])))
   #     up_j_sim.append(jaccard_similarity(set(res2[str(channel_id_verified)][0:10]), set(res2[str(channel_id_fake)][0:10])))

       
   
#print % of very similar,similar,pretty similar,different,completely different channels
def similarity(distance):
   print(distance)
   print(np.mean(distance))

   cont1=0
   cont2=0
   cont3=0
   cont4=0
   cont5=0
   tot=len(distance)
   for i in distance:
     if i==1:
       cont1+=1
     elif i>0.9:
       cont2+=1
     elif i >0.5:
       cont3+=1
     elif i>0.3: 
       cont4+=1
     else:
       cont5+=1

   print(cont1*100/tot,cont2*100/tot,cont3*100/tot,cont4*100/tot,cont5*100/tot)


similarity(bio_similarities)
similarity(title_similarities)
similarity(username_similarities)

#boxplot of channels' similarities (bio,user e title)
bio_similarities.sort()
title_similarities.sort()
username_similarities.sort()
info= pd.Series()
similarity= pd.Series(bio_similarities+title_similarities+username_similarities)
l=len(bio_similarities)

df=pd.DataFrame({'info':info,'Levenshtein_normalized_similarity':similarity}) 
df['info'][0:l]='bio'    
df['info'][l+1:2*l]='title'
df['info'][2*l+1:]='username'
fig = px.box(df, x="info", y="Levenshtein_normalized_similarity",  width=800, height=400)

fig.show()

#boxplot of domains'similarity
j_sim.sort()
top_j_sim.sort()
up_j_sim.sort()

l=len(j_sim)
domain_set= pd.Series()
jaccard_similarity= pd.Series(j_sim+top_j_sim+up_j_sim)
df = pd.DataFrame({'Set': domain_set,'Jaccard similarity': jaccard_similarity})  
df['Set'][0:l]='with all domains'
df['Set'][l:2*l]='with top 20 domains'  #di ogni canale i 20 piu presenti
df['Set'][2*l:]='with domains over a threshold'  #di ogni canale quelli presenti piu volte del valore medio delle occoronze di tutti domini del canale

fig1 = px.box(df, x='Set', y="Jaccard similarity", width=800, height=400)
fig1.show()


