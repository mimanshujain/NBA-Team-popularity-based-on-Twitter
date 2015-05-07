#!/usr/bin/env python

"""

Use Twitter API to grab tweets using hashtags; 
export text file

Uses Twython module to access Twitter API

"""
import twitter
import sys
import string
import os
import simplejson #install simplejson at https://pypi.python.org/pypi/simplejson/
from twython import Twython #install Twython at https://github.com/ryanmcgrath/twython

#WE WILL USE THE VARIABLES DAY, MONTH, AND YEAR FOR OUR OUTPUT FILE NAME
import datetime
now = datetime.datetime.now()
day=int(now.day)
month=int(now.month)
year=int(now.year)

CONSUMER_KEY = 'QIdzVCpO7U4VJ8EBt1S5nEjEy'
CONSUMER_SECRET = 'nvVLGMA5M2BcvjgZnJ6FxfuRRqTYFV4NkFLDY7txS3jFUaViOm'
OAUTH_TOKEN = '70677289-JBN42DMH1RMIc52G2HKa9IveJgIXYNOJbBmkOYfqB'
OAUTH_TOKEN_SECRET = 'xseT7QsumIDRIdByNiAXjbznTolzeiQEkjofFPRfbxJ1k'

auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                           CONSUMER_KEY, CONSUMER_SECRET)
twitter_api = twitter.Twitter(auth=auth)

path = os.getcwd()

# #FOR OAUTH AUTHENTICATION -- NEEDED TO ACCESS THE TWITTER API
# t = Twython(app_key='qtW8Q4270j67gooVD19tvAGo9',
#     app_secret='0Jnd7nIrLYv3BhZdiT98iKcaQKZBEipXziib0CitV2RZ6zXATQ',
#     oauth_token='2652772872-W9GTB3c973ayomnFPW1qEFgieNpskT5yJAD0c29',
#     oauth_token_secret='nHdpiqLHzQVGSfVqgM7JgFN89FSpdkJpLdTNX1YYskx0G')

hashtag = 'Rockets' ##### this line need to change
delimiter = ','
data = twitter_api.search.tweets(q='#'+hashtag, count=100)
tweets = data['statuses']
# print tweets

#NAME OUR OUTPUT FILE - %i WILL BE REPLACED BY CURRENT MONTH, DAY, AND YEAR
outfn = hashtag+".csv"
#
#NAMES FOR HEADER ROW IN OUTPUT FILE
fields = "created_at text".split()
#
#INITIALIZE OUTPUT FILE AND WRITE HEADER ROW
outfp = open(os.path.join(path,outfn), "w")
outfp.write(string.join(fields, ",") + "\n")  # comment out if don't need header

for entry in tweets:

    r = {}
    for f in fields:
        r[f] = ""
    #ASSIGN VALUE OF 'ID' FIELD IN JSON TO 'ID' FIELD IN OUR DICTIONARY
    r['created_at'] = entry['created_at']
    text = entry['text']
    for ch in [',', '&amp', ';', '\n', '\t']:
        text = text.replace(ch, " ")
    r['text'] = text

    print (r)
    #CREATE EMPTY LIST
    lst = []
    #ADD DATA FOR EACH VARIABLE
    for f in fields:
        lst.append(unicode(r[f]).replace("\/", "/"))
    #WRITE ROW WITH DATA IN LIST
    outfp.write(string.join(lst, delimiter).encode("utf-8") + "\n")

outfp.close()
