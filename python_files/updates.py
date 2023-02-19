
from collect import Database
from url import plot_cdf
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
from matplotlib.colors import ListedColormap
from rapidfuzz.distance import Levenshtein
from dateutil import parser
from collections import Counter

with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

db = Database(
    "dbname='telegramdb' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)

mappings= db.get_mappings() 

exluded_channels= db.get_excluded()
exluded_channels=[i[0] for i in exluded_channels]

res = db.count_updates(exluded_channels)
title_updates= [(str(i[0]),i[1]) for i in res[0]]
title_updates= dict(title_updates)
photo_updates= [(str(i[0]),i[1]) for i in res[1]]
photo_updates= dict(photo_updates)
channel_creation= [(str(i[0]),i[1]) for i in res[2]]
channel_creation= dict(channel_creation)

data = pd.DataFrame(res[3], columns=['channel_id', 'title'])

num_messages_verified= db.get_channel_number_messages("true", exluded_channels)
num_messages_fake= db.get_channel_number_messages("false", exluded_channels)

dif_title_updates=[]
dif_photo_updates=[]
count_no_real_title_updates=0
count_no_fake_title_updates=0
count_no_title_updates=0
count_no_real_photo_updates=0
count_no_fake_photo_updates=0
count_no_photo_updates=0

   
dif_creation=[]
titles_similarity_ratio=[]
dic=dict()

for m in mappings:  
      channel_id_real=str(m[0])
      channel_id_fake=str(m[1])

      if channel_id_real not in title_updates.keys() and channel_id_fake in title_updates.keys():
         title_updates[channel_id_real]=0
         count_no_real_title_updates+=1
      if channel_id_fake not in title_updates.keys() and channel_id_real in title_updates.keys():
         count_no_fake_title_updates+=1
         title_updates[channel_id_fake]=0
      if channel_id_fake not in title_updates.keys() and channel_id_real not in title_updates.keys():
         count_no_title_updates+=1
         title_updates[channel_id_real]=0
         title_updates[channel_id_fake]=0
      dif_title_updates.append(title_updates[channel_id_real]-title_updates[channel_id_fake])
      
      
      if channel_id_real in photo_updates.keys() and channel_id_fake in photo_updates.keys():
         dif_photo_updates.append(photo_updates[channel_id_real]/photo_updates[channel_id_fake])
      
      if channel_id_real not in photo_updates.keys() and channel_id_fake in photo_updates.keys():
         count_no_fake_photo_updates+=1
      if channel_id_fake not in photo_updates.keys() and channel_id_real in photo_updates.keys():
         count_no_fake_photo_updates+=1
      if channel_id_real not in photo_updates.keys() and channel_id_fake not in photo_updates.keys():
         count_no_photo_updates+=1

   
      d1 = channel_creation[channel_id_real] 
      date1 = parser.parse(d1)
      d2 = channel_creation[channel_id_fake] 
      date2 = parser.parse(d2)
      delta = date2 - date1
      dif_creation.append(delta.days)

      real_titles_similarity= []
      fake_titles_similarity= []

      real_titles_history= data.loc[data['channel_id'] == m[0]].iloc[:,1].tolist()
      if len(real_titles_history)>1: #prendo solo quelli in cui ci sono title updates (sia real)
       
        for i,title in enumerate(real_titles_history[1:]):
          real_titles_similarity.append(Levenshtein.normalized_similarity(title,real_titles_history[i]))
          
      fake_titles_history= data.loc[data['channel_id'] == m[1]].iloc[:,1].tolist()
      if len(fake_titles_history)>1: #che fake, tamto la %la segno nel barchart
     
        for i,title in enumerate(fake_titles_history[1:]):
          fake_titles_similarity.append(Levenshtein.normalized_similarity(title,fake_titles_history[i]))
      
      if real_titles_similarity or fake_titles_similarity:  #non mostro solo se antrambi canali hanno 0 updates
        N=5
        real_titles_similarity+= [np.nan] * (N - len(real_titles_similarity))
        fake_titles_similarity+= [np.nan] * (N - len(fake_titles_similarity))
        dic[channel_id_real+'-'+channel_id_fake]= real_titles_similarity+fake_titles_similarity
'''   
df=pd.DataFrame.from_dict(dic,orient='index')
fig1, ax1 = plt.subplots(figsize=(19,10))        
sb.heatmap(df, annot=True, vmin=0, vmax=1, linewidths=.5, ax=ax1)
sb.heatmap(
    np.where(df.isna(), 0, np.nan),
    ax=ax1,
    cbar=False,
    annot=np.full_like(df, "NA", dtype=object),
    fmt="",
    cmap=ListedColormap(['white']),
    annot_kws={"size": 10, "va": "center_baseline", "color": "black"},
    linecolor="black",
    linewidth=0.5, 
    xticklabels= ['update1', 'update2','update3','update4','update5','update1','update2','update3','update4','update5'],
    yticklabels= dic.keys())

ax1.set_title("Levenshtein-normalized similarities in the real and fake channels' titles history",fontsize=17, pad=20)
ax1.set_xlabel('Real channel                                                                                    Fake channel', fontsize=13)
ax1.set_ylabel("Real-Fake channels' IDs", fontsize=13)

fig1.savefig('titles_history_similarity')
''' 

list_of_delta = [dif_photo_updates]


c = Counter(dif_title_updates)
percentage= [i*100/122 for i in c.values()]

plt.figure()
plt.bar(c.keys(),percentage)
plt.xticks(np.arange(-5,6,1))
plt.xlabel("Difference of title updates")
plt.ylabel("% of channels'pairs")
plt.title("Title updates' study between real and fake channels")
plt.savefig('title_updates')
plt.close()

plt.figure(figsize=(13,6))
X = ['Only in real channel 0','Only in fake channel 0','Both channels 0','Both different from 0']
a= count_no_real_title_updates*100/122
b= count_no_fake_title_updates*100/122
c= count_no_title_updates*100/122
d= 100-a-b-c
Y= [a,b,c,d]
a= count_no_real_photo_updates*100/122
b= count_no_fake_photo_updates*100/122
c= count_no_photo_updates*100/122
d= 100-a-b-c
Z= [a,b,c,d]
  
X_axis = np.arange(len(X))
  
plt.bar(X_axis - 0.2, Y, 0.4, label = 'Title')
plt.bar(X_axis + 0.2, Z, 0.4, label = 'Photo')
plt.xticks(X_axis, X)
plt.xlabel("Number of updates")
plt.ylabel("% of channel pairs")
plt.title("Title and and photo updates'study between real and fake channels")
plt.legend()
plt.savefig('title_photo_updates')
plt.close() 
'''
plot_cdf(list_of_delta, 
        '# of photo updates in the real channel / photo updates in the fake channel',
        path='photo_updates.png',
        islogx=True)


dif_creation.sort()
fig = plt.figure(figsize =(8, 6))
 
# Creating plot
bp= plt.boxplot(dif_creation, labels = ["Real-Fake channel pair"])
outliers = [flier.get_ydata() for flier in bp["fliers"]]
boxes = [box.get_ydata() for box in bp["boxes"]]
medians = [median.get_ydata() for median in bp["medians"]]
whiskers = [whiskers.get_ydata() for whiskers in bp["whiskers"]]

print("Outliers: ", outliers)
print("Boxes: ", boxes)
print("Medians: ", medians)
print("Whiskers: ", whiskers)
plt.title('Distribution of the time interval between the real and fake channel creation')
plt.ylabel('Days')
fig.savefig('days_interval')
'''







