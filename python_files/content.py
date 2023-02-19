import pandas as pd
from collect import PgConn, Database, SyncTelegramClient
import yaml
import re
import flag
import emoji

import tldextract

from urlextract import URLExtract



telethon_api = SyncTelegramClient()
with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

db = Database(
    "dbname='telegramdb' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)

'''#filter message from urls and emojis
exluded_channels= db.get_excluded()
exluded_channels=[i[0] for i in exluded_channels]

messages= db.get_messages(exluded_channels)


def get_emoji_regexp():
    # Sort emoji by length to make sure multi-character emojis are
    # matched first
    emojis = sorted(emoji.EMOJI_DATA, key=len, reverse=True)
    pattern = u'(' + u'|'.join(re.escape(u) for u in emojis) + u')'
    return re.compile(pattern)

exp = get_emoji_regexp()


messages_without_emoji_url=[]

extractor = URLExtract()

for index,m in enumerate(messages): #visto che erano tanti mess li ho fatti un po alla volta  non sso perchhe non mi ha tolto tutti gli url, li ho ricacciati i restanti da jupyter
   text=m[1]
   for u in extractor.find_urls(text):
       text=text.replace(u, ' ')
   text=flag.dflagize(text)
   text=exp.sub(repl=' ', string=text)
   
   messages_without_emoji_url.append(tuple([m[0],text]))


data = pd.DataFrame(messages_without_emoji_url, columns=['channel_id', 'message_text'])
data.to_csv('messages_text.csv') # e ho creato tanti dataframe che poi ho unito e caricato su colab per calcolare gli embeddingd dai messaggi
'''

#save channel pairs into a file
mappings= db.get_mappings() 
with open('channel_pair.txt', 'w') as fp:
    fp.write('\n'.join('%s %s' % x for x in mappings))





