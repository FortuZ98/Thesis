import pandas as pd
from collect import Database
import yaml
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
num_messages_verified= db.get_channel_number_messages("true", exluded_channels)
num_messages_fake= db.get_channel_number_messages("false", exluded_channels)

url_count= db.get_url_count(exluded_channels)
url_count= dict((x, y) for x, y in url_count)
ver_ratio=[]
fake_ratio=[]

for i in num_messages_verified:
   ver_ratio.append(url_count.get(i[0],0)*100/i[1])

for i in num_messages_fake:
   fake_ratio.append(url_count.get(i[0],0)*100/i[1])

  

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
    list_counts= [i[:] for i in list_of_counts]
    while(len(list_counts) > len(colors)):
        colors1=colors
        shuffle(colors)
        colors = colors + colors1
        line_styles1=line_styles
        shuffle(line_styles)
        line_styles = line_styles + line_styles1
        
    if xlimit:
        l2 = []
        for l in list_counts:
            l2_1 = [x for x in l if x<=xlimit]
            l2.append(l2_1)
        list_counts = l2
    
    for l in list_counts: #farlo con una copia non cambiare ordine rle
      
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
    

list_of_counts = [ver_ratio, fake_ratio]

plot_cdf(list_of_counts, 
        '# of messages with urls/ total messages',
        leg=['Verified channels', 'Fake channels'],
        path='figures/cdf_mess_url.png',
        islogx=False)











