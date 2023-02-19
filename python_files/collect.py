from collections import namedtuple
from telethon.errors import ChatAdminRequiredError, ChannelInvalidError, ChannelPrivateError, FloodWaitError
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.errors.rpcerrorlist import ChannelPrivateError
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon.tl.types import InputPeerPhotoFileLocation
from telethon.tl.functions import upload
import logging
import yaml
import pandas as pd
import traceback
import json
import time
from time import sleep
import psycopg2
from psycopg2.errorcodes import UNIQUE_VIOLATION
import os


logging.basicConfig(level=logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

logger = logging.getLogger("Log")

MESSAGE_FIELDS = [
    "record_id", "message_id", "channel_id", "data"]
                
Message = namedtuple("Message", MESSAGE_FIELDS)

CHANNEL_FIELDS = [
    "channel_id", "channel_name", "channel_link"]    

Channel = namedtuple("Channel", CHANNEL_FIELDS)

class PgConn:
    def __init__(self, conn, conn_str):
        self.conn = conn
        self.conn_str = conn_str
        self.cur = conn.cursor()
    def __enter__(self):
        return self
    def exec(self, query_string, args=None):
        if args is None:
            args = []
        while True:
            try:
                if config["sql_debug"]:
                    logger.debug(query_string)
                    logger.debug("With args " + str(args))

                self.cur.execute(query_string, args)
                break
            except psycopg2.Error as e:
                if e.pgcode == UNIQUE_VIOLATION:
                    break
                traceback.print_stack()
                self._handle_err(e, query_string, args)

    def query(self, query_string, args=None):
        while True:
            try:
                if config["sql_debug"]:
                    logger.debug(query_string)
                    logger.debug("With args " + str(args))

                self.cur.execute(query_string, args)
                res = self.cur.fetchall()

                if config["sql_debug"]:
                    logger.debug("result: " + str(res))

                return res
            except psycopg2.Error as e:
                if e.pgcode == UNIQUE_VIOLATION:
                    break
                self._handle_err(e, query_string, args)

    def _handle_err(self, err, query, args):
        logger.warn("Error during query '%s' with args %s: %s %s (%s)" % (query, args, type(err), err, err.pgcode))
        self.conn = psycopg2.connect(self.conn_str)
        self.cur = self.conn.cursor()
        sleep(0.1)
    def __exit__(self, type, value, traceback):
        try:
            self.conn.commit()
            self.cur.close()
        except:
            pass
class Database:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        
        print(self)
        print(self.conn_str)
        self.conn = psycopg2.connect(self.conn_str)

    def _get_conn(self):
        return PgConn(self.conn, self.conn_str)

    def insert_message(self, message: Message):
        with self._get_conn() as conn:
            conn.exec(
                "INSERT INTO message "
                "   (id, message_id, channel_id, data) "
                "VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (id) DO "
                "   UPDATE SET "
                "       data=EXCLUDED.data",
                (
                    message.record_id, message.message_id, message.channel_id, message.data
                )
            )

    def insert_channel(self, channel):
        with self._get_conn() as conn:
            conn.exec(
                "INSERT INTO channel "
                "   (id, name, link) "
                "VALUES (%s,%s,%s) ",
                (
                    channel.channel_id, 
                    channel.channel_name, channel.channel_link
                )
            )
    def insert_mapping(self, verified_channel_id, fake_channel_id):
        with self._get_conn() as conn:
            conn.exec(
                "INSERT INTO mapping "
                "   (verified_channel_id, fake_channel_id) "
                "VALUES (%s,%s) ",
                (
                    verified_channel_id, 
                    fake_channel_id,
                )
            )

    def insert_channel_data(self, channel_id, last_message_id, channel_data):
        if isinstance(channel_data, dict):
            data = json.dumps(channel_data)
        with self._get_conn() as conn:
            conn.exec(
                "UPDATE channel "
                "SET last_message_id = %s, "
                "data = %s "
                "WHERE id=%s ",
                (last_message_id, data, channel_id)
            )

    def get_channel_by_id(self, channel_id):
        with self._get_conn() as conn:
            res = conn.query(
                "SELECT * FROM channel WHERE id = %s",
                (channel_id)
            )
        return None if not res else res[0]

    def get_channel_data(self, official, list_id):       #sostituire con data->'full_chat'->'participants_count per semplificare query e restituire direttamente solo il num.di subscribers e chiamarlo get_channel_subscribers
        with self._get_conn() as conn:
            res = conn.query(
                "select channel.id, data, link from channel, (select id,item_object->>'verified' as verified from (SELECT id, arr.position,arr.item_object "
                "FROM (select id,data -> 'chats' as chats from channel) as c, "
                "jsonb_array_elements(chats) with ordinality arr(item_object, position) where position=1) as t) as x "
                "where channel.id=x.id and verified=%s and channel.id not in %s  order by channel.id ",
                (official, tuple(list_id))
            )
        return None if not res else res

    def get_channel_number_messages(self, official, list_id):
        with self._get_conn() as conn:
            res = conn.query(
                "select channel_id, count(*) from message, (select id as i,item_object->>'verified' as verified from (SELECT id, arr.position,arr.item_object "   #cambiare analize perche ho aggiunto channel_id
                "FROM (select id,data -> 'chats' as chats from channel where id not in %s) as c, "
                "jsonb_array_elements(chats) with ordinality arr(item_object, position) where position=1) as t) as r "
                "where channel_id=i and verified=%s and message.id in (select id from message "
                "where (data->>'_')='Message' and strpos((data->>'message'), 'be displayed because it violated local laws')=0) group by channel_id order by channel_id ",
                (tuple(list_id), official)
            )
        return None if not res else res 

    def get_messages_info(self, list_id):
        with self._get_conn() as conn:
            res = conn.query(
                " select channel_id, data->'forwards' as forwards, data->'views' as views from message where channel_id in %s and (data->>'_')='Message' "
                " and strpos((data->>'message'), 'be displayed because it violated local laws')=0 ",
                ([tuple(list_id)]) 

            )
        return None if not res else res

    def get_channel_links(self,list_id):
        with self._get_conn() as conn:
            res = conn.query(
                "SELECT link FROM channel where id in %s ",
                ([tuple(list_id)]) 
            )
        return None if not res else res

    def get_excluded(self):
        with self._get_conn() as conn:
            res = conn.query(
                "select channel_id from message group by channel_id having count(*)<20 "
            )
        return None if not res else res 

    def get_mappings(self):
        with self._get_conn() as conn:
            res = conn.query(
                "select * from mapping "
            )
        return None if not res else res 

    def get_bio(self):
        with self._get_conn() as conn:
            res = conn.query(
                "select id, data->'full_chat'->>'about' as bio from channel "
            )
        return None if not res else res 
    
    def get_title_username(self):
        with self._get_conn() as conn:
            res = conn.query(
                "select id, data->'chats'->0->>'title' as title, data->'chats'->0->>'username' as user  from channel "
            )
        return None if not res else res 


    def get_urls(self,list_id):
        with self._get_conn() as conn:
            res = conn.query(
"select ch_id, string_agg(entities->>'url', ', ') as urls "
"from (select ch_id, jsonb_array_elements(e) as entities from(select channel_id as ch_id, data->'entities' as e from message) as t) as y "
"where (entities->>'_')='MessageEntityTextUrl' and ch_id not in %s  group by ch_id ",
        ([tuple(list_id)])
            )
        return None if not res else res 
    


    def get_url_messages(self,list_id):
        with self._get_conn() as conn:
            res = conn.query(
"select distinct on (channel_id, message_id) channel_id, message_id, data->>'message' "
"from message, (select id as i,jsonb_array_elements(e) as entities from(select id, data->'entities' as e from message order by channel_id) as t) as y  "
"where message.id=i and (entities->>'_')='MessageEntityUrl' and channel_id not in %s   ",
        ([tuple(list_id)])
            )
        return None if not res else res 


    
    def get_url_count(self,list_id):
        with self._get_conn() as conn:
            res = conn.query(
"select channel_id, count(*) from (select distinct channel_id,message_id "
"from message,(select id as i,jsonb_array_elements(e) as entities from(select id, data->'entities' as e from message) as t) as y "
"where message.id=i and ((entities->>'_')='MessageEntityUrl' or (entities->>'_')='MessageEntityTextUrl') and channel_id not in %s) as r  group by channel_id ; ",
        ([tuple(list_id)])
            )
        return None if not res else res 

    def get_messages(self,list_id):
        with self._get_conn() as conn:
            res = conn.query(
"select channel_id, data->>'message' as m from message "
"where (data->>'_')='Message' and strpos((data->>'message'), 'be displayed because it violated local laws')=0 and channel_id not in %s ",
        ([tuple(list_id)])
            )
        return None if not res else res 

    def get_channel_activity(self, list_id):
        with self._get_conn() as conn:
            res = conn.query(
"select channel_id, (data->>'date')::date as timestamp, count(*) from message "
"where channel_id not in %s and (data->>'_')='Message' and strpos((data->>'message'), 'be displayed because it violated local laws')=0  "
"group by channel_id, timestamp order by channel_id, timestamp ",
         ([tuple(list_id)])
)
        return None if not res else res 
  

    def get_user(self, channel_id):
        with self._get_conn() as conn:
            res = conn.query(
                "select data->'chats'->0->'username' from channel where id= %s ",
                ([channel_id])
            )
        return None if not res else res[0][0]


    def count_updates(self, list_id):
        with self._get_conn() as conn:
            res1 = conn.query(
                "select channel_id,count(*) from message where (data->'action'->>'_')='MessageActionChatEditTitle' and channel_id not in %s group by channel_id ",
                ([tuple(list_id)])
            )
            res2 = conn.query(
                "select channel_id,count(*) from message where (data->'action'->>'_')='MessageActionChatEditPhoto' and channel_id not in %s group by channel_id ",
                ([tuple(list_id)])
            )
            res3 = conn.query(
                "select channel_id, data->'date' as date from message where (data->'action'->>'_')='MessageActionChannelCreate' and channel_id not in %s ",
                ([tuple(list_id)])
            )
            res4 = conn.query(
                "select channel_id, data->'action'->'title' from message where ((data->'action'->>'_')='MessageActionChatEditTitle' "
                "or (data->'action'->>'_')='MessageActionChannelCreate') and channel_id not in %s  order by data->'date' ",
                ([tuple(list_id)])
            )
            res=[res1,res2,res3,res4]
        return None if not res else res


 
print("start")


with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

class SyncTelegramClient:
    def __init__(self):
        self._client = TelegramClient("Session12", config["api_id"], config["api_hash"])

    def fetch_messages(self, channel_link: str, channel_id: int, channel_name: str, min_id: int, channel_data): #minid
        """Method to fetch messages from a specific channel / group"""

        logger.debug("Fetching all the messages from channel %s" % (channel_name))
       
        with self._client as client:
            total_messages=0
            message_id=min_id
            messages = client.iter_messages(channel_link, reverse=True, min_id= min_id)  
            
            for m in messages:
              message_id = m.id
              message_channel_id = m.to_id.channel_id
              if message_channel_id != channel_id:
                logger.warning("Message channel id for %s does not match"
                               "expected value. %d != %d" %
                               (channel_name, message_channel_id, channel_id))
              record_id = (message_channel_id << 32) + message_id
              data = m.to_json()
              total_messages+=1

              db.insert_message(Message(
                record_id=record_id,
                message_id=message_id,
                channel_id=channel_id,
                data=data,
            )) 
            last_message_id= message_id

            if total_messages > 0:
                db.insert_channel_data(channel_id, last_message_id, channel_data)
            
     
    def get_channel_info(self, channel_link):
        info="" 
        with self._client as client:
          try:
             info = client(functions.channels.GetFullChannelRequest(channel=channel_link)).to_json()
          except FloodWaitError as e:
                   print('Flood wait for ', e.seconds)  
          except:
             logger.warning("The channel %s doesn't exist anymore" %(channel_link))
             pass 
              
        return info


    def save_channel(self, channel_link, info, index, channels_count, verified_channels_count, fake_channels_count):
          channel_data= json.loads(info)
          
          channels_count+=1

 
          verified= channel_data["chats"][0]["verified"]
          if verified!= marks[index]:
            if verified:
              logger.warning("Write down on the csv file that the channel %s is verified" %(channel_link))
            else:
              logger.warning("Write down on the csv file that the channel %s is fake" %(channel_link))
          if verified:
              verified_channels_count+=1
          else:
              fake_channels_count+=1
          channel_id = channel_data["full_chat"]["id"]
          channel_name = channel_data["chats"][0]["title"]
          print(channel_name)
          channel_on_DB = db.get_channel_by_id(channel_id)
          if channel_on_DB is None:
            db.insert_channel(Channel(
                channel_id=channel_id,
                channel_name=channel_name,
                channel_link=channel_link
            ))
            telethon_api.fetch_messages(
            channel_link, channel_id, channel_name,-1, channel_data)
          else:
            telethon_api.fetch_messages(
            channel_link, channel_id, channel_name, channel_on_DB[3], channel_data)

          return channels_count, verified_channels_count, fake_channels_count

if __name__ == "__main__": 
  
  channels_count=0
  verified_channels_count=0
  fake_channels_count=0

  # reading CSV file
  data = pd.read_csv("Channels.csv", names=['url', 'official'])
  # converting column data to list
  channels = data['url'].tolist()
  marks = data['official'].tolist()
  db = Database(
    "dbname='telegramdb' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)


  telethon_api = SyncTelegramClient()

  for channel in channels:
    index= channels.index(channel)
    if index!=len(channels)-1:
      if marks[index]==False or marks[index+1]==False :   #add only the fakes and the verified with one fake
        info = telethon_api.get_channel_info(channel)
        if info!="":
          channels_count, verified_channels_count, fake_channels_count= telethon_api.save_channel(channel, info, index, channels_count, verified_channels_count, fake_channels_count)
      
    elif marks[index]==False:  #last value of list
        info = telethon_api.get_channel_info(channel)
        if info!="":
          
          channels_count, verified_channels_count, fake_channels_count= telethon_api.save_channel(channel, info, index, channels_count, verified_channels_count, fake_channels_count)
             
               
  logger.debug("The number of the saved channels is %s, including %s fakes and %s verified ones with at least one corresponding fake"
               % (channels_count, fake_channels_count, verified_channels_count)) 

   

  


