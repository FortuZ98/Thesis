import pandas as pd
from collect import PgConn, Database
import psycopg2
import yaml
import json
import matplotlib 
import numpy as np
import matplotlib.pyplot as plt
from random import shuffle


with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)
db = Database(
    "dbname='telegramdb' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)

exluded_channels= db.get_excluded()
exluded_channels=[i[0] for i in exluded_channels]

data_verified= db.get_channel_data("true", exluded_channels)
data_fake= db.get_channel_data("false", exluded_channels)

num_subscribers_verified=[]
num_subscribers_fake=[]
channels_verified=[]
channels_fake=[]

if data_verified!=None:
   for d1 in data_verified:
      channel_id_verified=d1[0]
      channel_data_verified= d1[1]["full_chat"]["participants_count"]
      num_subscribers_verified.append(channel_data_verified)
      channels_verified.append(channel_id_verified)
if data_fake!=None:
   for d2 in data_fake:
      channel_id_fake=d2[0]
      channel_data_fake= d2[1]["full_chat"]["participants_count"]
      num_subscribers_fake.append(channel_data_fake)
      channels_fake.append(channel_id_fake)

num_messages_verified= db.get_channel_number_messages("true", exluded_channels)
num_messages_verified= [i[1] for i in num_messages_verified]]
num_messages_fake= db.get_channel_number_messages("false", exluded_channels)
num_messages_fake= [i[1] for i in num_messages_fake]]

messages_verified_info = db.get_messages_info(channels_verified)
v= list(map(list, zip(*messages_verified_info)))
channel_ids_v=v[0]
messages_forwards_v=v[1]
messages_views_v=v[2]

messages_fake_info= db.get_messages_info(channels_fake)
f= list(map(list, zip(*messages_fake_info)))
channel_ids_f=f[0]
messages_forwards_f=f[1]
messages_views_f=f[2]


def plot_cdf(list_of_counts, xlabel, path, leg=False, islogx=True, xlimit=False):
    t_col = "#235dba"
    g_col = "#005916"
    c_col = "#a50808"
    r_col = "#ff9900"
    black = "#000000"
    pink = "#f442f1"
    t_ls = '-'
    r_ls = '--'
    c_ls = ':'
    g_ls = '-.'

    markers = [".", "o", "v", "^", "<", ">", "1", "2"]
    colors = [t_col, c_col, g_col, r_col, black, 'c', 'm', pink]
    line_styles = [t_ls, r_ls, c_ls, g_ls,t_ls, r_ls, c_ls, g_ls, t_ls]
    colors = colors[1:]
    line_styles= line_styles[1:]
    list_counts= [i[:] for i in list_of_counts] #copia
    while(len(list_counts) > len(colors)):
        colors = colors + shuffle(colors)
        line_styles = line_styles + shuffle(line_styles)
        
    if xlimit:
        l2 = []
        for l in list_counts:
            l2_1 = [x for x in l if x<=xlimit]
            l2.append(l2_1)
        list_counts = l2
    
    for l in list_counts: #fatto con una copia non cambiare ordine della lista originale      
        l.sort()

    fig, ax = plt.subplots(figsize=(6,4))
    yvals = []
    for l in list_counts:
        yvals.append(np.arange(len(l))/float(len(l)-1))
    for i in range(len(list_counts)):
        ax.plot(list_counts[i], yvals[i], color=colors[i], linestyle=line_styles[i])
    if islogx:
        ax.set_xscale("log")
    plt.xlabel(xlabel)
    plt.ylabel('CDF')
    plt.grid()
    for item in ([ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(13)
    
    if leg:
        plt.legend(leg, loc='best', fontsize=13)
    
    
    fig.savefig(path, bbox_inches='tight')
    
list_of_counts_subscribers = [num_subscribers_verified, num_subscribers_fake]

plot_cdf(list_of_counts_subscribers, 
        '# of subscribers',
        leg=['Verified channels', 'Fake channels'],
        path='figures/cdf_subscribers.png',
        islogx=True)

list_of_counts_messages= [num_messages_verified, num_messages_fake]

plot_cdf(list_of_counts_messages, 
        '# of messages',
        leg=['Verified channels', 'Fake channels'],
        path='figures/cdf_messages.png',
        islogx=True)

list_of_forwards= [messages_forwards_v, messages_forwards_f]
# se runno pure sta parte poi forse mi fa il sort di messages_forward e mi fa cambiare il grafico, penso sia giusto quello che esce lascianod sta parte commentata, risolto
plot_cdf(list_of_forwards, 
        '# of forwards',
        leg=['Verified channels messages', 'Fake channels messages'],
        path='figures/cdf_messages1_forwards.png',
        islogx=True)

list_of_views= [messages_views_v, messages_views_f]

plot_cdf(list_of_views, 
        '# of views',
        leg=['Verified channels messages', 'Fake channels messages'],
        path='figures/cdf_messages_views.png',
        islogx=True)


data = pd.read_csv("Channels.csv", names=['url', 'official'])
  
channels = data['url'].tolist() 
marks = data['official'].tolist()
mapping = dict()
links= db.get_channel_links(channels_verified)
  
  
for channel in links:
  l=[]
  index= channels.index(channel[0]) +1
  while marks[index]==False: 
        l.append(channels[index])
        index=index+1
  mapping[channel[0]]=l



list_of_ratio_subscribers=[]
fake_ids=[]
list_of_ratio__mean_forwards=[]
list_of_ratio__median_forwards=[]
list_of_ratio__mean_views=[]
list_of_ratio__mediaan_views=[]
list_of_ratio_num_messages=[]
f_real=[]
f_fake=[]
v_real=[]
v_fake=[]
mess_real=[]
mess_fake=[]

for d1 in data_verified:
      n_real= d1[1]["full_chat"]["participants_count"]
      mess_real= num_messages_verified[data_verified.index(d1)][0]
     
      for i,j in enumerate(channel_ids_v): #risolvibile con order by forse
        if j==d1[0]:
          f_real.append(messages_forwards_v[i])
          v_real.append(messages_views_v[i])
      
      mean_f1= np.mean(f_real)
      median_f1= np.median(f_real)     
      mean_v1= np.mean(v_real)
      median_v1= np.median(v_real)
      f_real=[]
      v_real=[]
      link=d1[2]
      x=mapping[link]

      count=0
      for d2 in data_fake:
        if d2[2] in fake_ids:
          count+=1
          n_fake=d2[1]["full_chat"]["participants_count"]
          mess_fake= num_messages_fake[data_fake.index(d2)][0]
          list_of_ratio_subscribers.append(n_real/n_fake)
          list_of_ratio_num_messages.append(mess_real/mess_fake)
          
          for i,j in enumerate(channel_ids_f):
            if j==d2[0]:
              db.insert_mapping(d1[0],d2[0])
              f_fake.append(messages_forwards_f[i])
              v_fake.append(messages_views_f[i])
          mean_f2= np.mean(f_fake)
          median_f2= np.median(f_fake)
          mean_v2= np.mean(v_fake)
          median_v2= np.median(v_fake)
          f_fake=[]
          v_fake=[]

          list_of_ratio__mean_forwards.append(mean_f1/mean_f2)
          list_of_ratio__median_forwards.append(median_f1/median_f2)
          list_of_ratio__mean_views.append(mean_v1/mean_v2)
          list_of_ratio__medianan_views.append(median_v1/median_v2)
          if count==len(x):
            break

list_of_ratio_forwards=[list_of_ratio__mean_forwards] #list_of_ratio__median_forwards
list_of_ratio_views=[list_of_ratio__mean_views] #,list_of_ratio__median_views escluso per infiniti values

plot_cdf([list_of_ratio_subscribers], 
        '# of number of subscribers to the real channel / \n number of subscribers to the fake channel',
        path='figures/cdf_ratio_subscribers.png',
        islogx=True)

plot_cdf([list_of_ratio_num_messages], 
        '# of number of messages of the real channel / \n number of messages of the fake channel',
        path='figures/cdf_ratio_messages.png',
        islogx=True)

plot_cdf(list_of_ratio_forwards, 
        '# of average of the number of message forwards from a real channel / \n average of the number of message forwards from a fake channel',
        path='figures/cdf_ratio_forwards.png',
        islogx=True)

plot_cdf(list_of_ratio_views, 
        '# of average number of message views from a real channel / \n average number of message views from a fake channel',
        path='figures/cdf_ratio_views.png',
        islogx=True)

