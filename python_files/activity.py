
from collect import PgConn, Database, SyncTelegramClient
import yaml
import numpy as np
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
from matplotlib.colors import ListedColormap


telethon_api = SyncTelegramClient()
with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

db = Database(
    "dbname='telegramdb' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)

mappings= db.get_mappings() 

exluded_channels= db.get_excluded()
exluded_channels=[i[0] for i in exluded_channels]

data = db.get_channel_activity(exluded_channels)

df = pd.DataFrame(data, columns = ['Channel id', 'Date', 'Messages'])

'''#zelensky channels activity
df_zelenskiy_real= df.loc[df['Channel id'] == 1463721328]
df_zelenskiy_fake= df.loc[df['Channel id'] == 1666349486]
del df_zelenskiy_real['Channel id']
del df_zelenskiy_fake['Channel id']
# Set the Date as Index

df_zelenskiy_real['Date'] = pd.to_datetime(df_zelenskiy_real['Date'])
df_zelenskiy_fake['Date'] = pd.to_datetime(df_zelenskiy_fake['Date'])

df_zelenskiy_real.set_index(df_zelenskiy_real.Date, inplace=True)
df_zelenskiy_fake.set_index(df_zelenskiy_fake.Date, inplace=True)
df_zelenskiy_real = df_zelenskiy_real.resample('D').sum().fillna(0)
df_zelenskiy_fake= df_zelenskiy_fake.resample('D').sum().fillna(0)

df_zelenskiy_real.plot(figsize=(15, 6))
# adding title to the plot
plt.title("Zelenskiy's real channel activity")
  
# adding Label to the x-axis
plt.xlabel('Date')

plt.savefig('zelenskiy_real_channel_activity')

df_zelenskiy_real= df_zelenskiy_real.rename(columns={'Messages': 'Real channel'})
df_zelenskiy_real["Fake channel"] = df_zelenskiy_fake.Messages
  
# removing Missing Values
df_zelenskiy_real.dropna(inplace=True)

plt.figure(figsize=(16, 8), dpi=150)


df_zelenskiy_real.plot(color=['green', 'orange'])

plt.title("Zelenskiy's channels activity")

plt.xlabel('Date')
plt.ylabel('Number of messages') 
plt.legend()
plt.tight_layout()
plt.savefig('zelenskiy_channels_activity')


'''

corr_list=[]
dic=dict()
for m in mappings:  
      channel_id_real=m[0]
      channel_id_fake=m[1]
      df_real= df.loc[df['Channel id'] == channel_id_real]
      df_fake= df.loc[df['Channel id'] == channel_id_fake]

      del df_real['Channel id']
      del df_fake['Channel id']

      df_real['Date'] = pd.to_datetime(df_real['Date'])
      df_fake['Date'] = pd.to_datetime(df_fake['Date'])

      df_real.set_index(df_real.Date, inplace=True)
      df_fake.set_index(df_fake.Date, inplace=True)
      df_real = df_real.resample('D').sum().fillna(0)
      df_fake= df_fake.resample('D').sum().fillna(0)
      df_pair= df_real.rename(columns={'Messages': 'Real channel'})
      df_pair["Fake channel"] = df_fake.Messages
      df_pair.dropna(inplace=True)
   
      correlation=df_pair.corr(method='pearson').iloc[0,1]
      if df_pair.empty:
         correlation=10

      if dic.get(channel_id_real)== None:
        dic.update({channel_id_real: []})
      dic[channel_id_real].append(correlation)

      if correlation>0.7 and correlation<=1 or correlation < -0.2 :
         plt.figure(figsize=(16, 8), dpi=150)

         df_pair.plot(color=['green', 'orange'])
         title=""
         if correlation< -0.2:
           title="Unsynchronized channels' activity"
         else:
           title="Synchronized channels' activity"
         plt.title(title)

         plt.xlabel('Date')
         plt.ylabel('Number of messages') 
         plt.legend()
         plt.tight_layout()
         
         plt.savefig('channels_activity'+str(channel_id_fake))
      
df_corr=pd.DataFrame.from_dict(dic, orient='index') 


verified_usernames= []

for i in dic:
 verified_usernames.append(db.get_user(i))

df_corr= df_corr.round(4)
label= df_corr.replace(10, 'no common publication period')

fig1, ax1 = plt.subplots(figsize=(19,10))         
sb.heatmap(df_corr.iloc[0:37,0:3], annot=label.iloc[0:37,0:3], fmt='', vmin=-1, vmax=1, linewidths=.5, ax=ax1, yticklabels= verified_usernames[0:37])
sb.heatmap(
    np.where(df_corr.iloc[0:37,0:3].isna(), 0, np.nan),
    ax=ax1,
    cbar=False,
    annot=np.full_like(df_corr.iloc[0:37, 0:3], "NA", dtype=object),
    fmt="",
    cmap=ListedColormap(['white']),
    annot_kws={"size": 10, "va": "center_baseline", "color": "black"},
    linecolor="black",
    linewidth=0.5,  yticklabels= verified_usernames[0:37])

 
ax1.set_title("Pearson correlation between real and fake channel's messages",fontsize=17, pad=20)
ax1.set_xlabel('Fake channels', fontsize=13 )
ax1.set_ylabel("Real channel's usernames", fontsize=13)

fig1.savefig('correlation_activity5')
fig2, ax2 = plt.subplots(figsize=(19,10))         
sb.heatmap(df_corr[37:], annot=label.iloc[37:], fmt='', vmin=-1, vmax=1, linewidths=.5, ax=ax2, yticklabels= verified_usernames[37:], xticklabels=['Fake 1','Fake 2', 'Fake 3', 'Fake 4'])
sb.heatmap(
    np.where(df_corr[37:].isna(), 0, np.nan),
    ax=ax2,
    cbar=False,
    annot=np.full_like(df_corr[37:], "NA", dtype=object),
    fmt="",
    cmap=ListedColormap(['white']),
    annot_kws={"size": 10, "va": "center_baseline", "color": "black"},
    linecolor="black",
    linewidth=0.5 , yticklabels= verified_usernames[37:], xticklabels=['Fake 1','Fake 2', 'Fake 3', 'Fake 4'])

ax2.set_title("Pearson correlation between real and fake channel's messages",fontsize=17, pad=20)
ax2.set_xlabel('Fake channels', fontsize=13 )
ax2.set_ylabel("Real channel's usernames", fontsize=13)

fig2.savefig('correlation_activity6')

