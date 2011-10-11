#!/usr/bin/env python
from pymongo import Connection,objectid
import tweepy
from tweepy.auth import BasicAuthHandler
from tweepy.streaming import StreamListener, Stream
import datetime
import ConfigParser
import raindrop_utils

CONFIG_FILE = 'raindrop.cfg'
TWITTER_SCREEN_NAME = 'neilkod'

#uri = 'mongodb://neilkod:xyz@staff.mongohq.com:10055/earthquakes'
mongo_uri = raindrop_utils.get_mongo_uri(CONFIG_FILE)
connection = Connection(mongo_uri)
db=connection.earthquakes

KEYWORDS = ['earthquake','cnet','brewers','yankees','yankees']


# read the config file to get the twitter auth 
def get_twitter_config(config_file = CONFIG_FILE, screen_name = TWITTER_SCREEN_NAME):
  twitter_params = {}

  config = ConfigParser.ConfigParser()
  config.readfp(open(config_file))

  twitter_params['consumer_key'] = config.get('TwitterOauth','CONSUMER_KEY')
  twitter_params['consumer_secret'] = config.get('TwitterOauth','CONSUMER_SECRET')

  twitter_params['access_key'] = config.get(screen_name,'ACCESS_KEY')
  twitter_params['access_secret'] = config.get(screen_name,'ACCESS_SECRET')
  return twitter_params


#mongo --host staff.mongohq.com --port 10055 earthquakes -u neilkod -ppwd
#{ "_id" : ObjectId("4e545afe584424412eef3ddf"), "name" : "earthquake", "count" : 3 }


class Listener ( StreamListener ):
  def on_status( self, status ):

    # grab the date and time for the minute-level tracking
    # for the time being, store at the minute level
    # this may be too granular for plotting but useful for trending


    # round the time down to the 5-minute increment
    # example 9:43 would round down to 9:40
    # thanks to http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    tm = status.created_at
    tm = tm - datetime.timedelta(minutes=tm.minute % 5,
                                 seconds=tm.second,
                                 microseconds=tm.microsecond)

    date_string = tm.strftime('%Y%m%d')
    minute_string = tm.strftime('%H%M')
    timestamp = tm.strftime('%Y%m%d%H%M')
    print status.text
    print status.entities['urls']
    for keyword in KEYWORDS:
      if keyword in status.text.lower():
        # increment the counter
        db.keyword_total.update({'keyword':keyword} ,{'$inc': {"count":1}},True)
        try:
          db.keyword_period.update({'period': timestamp, 'keyword': keyword}, {'$inc':{'count':1}},True)
        except Exception as e:
          print "FAIL on upsert %s" % keyword, e

    return

  def on_error(self, status_code):
    print 'An error has occured! Status code = %s' % status_code
    return True  # keep stream alive
  def on_timeout(self):
    print 'timeout'

def main():
  try:
    config = get_twitter_config(CONFIG_FILE, TWITTER_SCREEN_NAME)
    auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
    auth.set_access_token(config['access_key'], config['access_secret'])
#    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
#    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    listener = Listener()
    stream = Stream(auth, listener, timeout=None,secure=1)
    #stream = Stream(USERNAME, PASSWORD, listener, timeout=None)
    # sample returns all tweets

    stream.filter(track = KEYWORDS)
  except KeyboardInterrupt:
    print '\nGoodbye!'

if __name__ == "__main__":
 main()
