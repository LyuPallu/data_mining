import tweepy
import sys
import collections
import random
import re

#random.seed(28)
# port_n = 9999
# output_file = 'o3.csv'
port_n = int(sys.argv[1])
output_file = sys.argv[2]

FIXED_SIZE = 100

class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        super(MyStreamListener, self).__init__()
        self.cnt = 0      
        self.tweet = []
        self.tags = collections.defaultdict(int)
        print('start')
        with open(output_file, 'w') as f:
            f.write('')
    
    def on_error(self, status_code):
        print("Error: ", str(status_code))
        if status_code == 420:
            print('wait and rerun')
            #returning False in on_error disconnects the stream
            return False

    def on_status(self, status):
        words = status.text
        words = re.sub(r'[^a-zA-Z0-9_#]'," ",words).split()

        if self.cnt < FIXED_SIZE:
            self.tweet.append(status.text) #del
            self.cnt += 1
            tmp_tags = []
            for w in words:
                if len(w)>1:
                    if w[0] == '#':
                        tmp_tags.append(w[1:])
            for tag in tmp_tags:
                self.tags[tag] += 1
            
            req = []
            for i in list(self.tags.items()):
                req.append(i[1])
            req = list(set(req))
            req_ = sorted(req, reverse = True)[:3]
            fin = []
            for i in list(self.tags.items()):
                if i[1] in req_:
                    fin.append(i)
            fin = sorted(fin, key=lambda x:(-1*x[1],x[0]))
            with open(output_file, 'a') as f:
                f.write(
                    'The number of tweets with tags from the begining: '+ str(self.cnt) + '\n')
                for item in fin:
                    f.write(str(item[0]) + ':' + str(item[1]) + '\n')
                f.write('\n')
        
        else:
            randomId = random.randint(0,self.cnt-1)
            if randomId < FIXED_SIZE:
                tmp_tags = []
                for w in words:
                    if len(w)>1:
                        if w[0] == '#':
                            tmp_tags.append(w[1:])
                if tmp_tags:
                    self.cnt += 1
                    tag_list = self.tweet.pop(randomId)
                    tmp_tags_ = []
                    for tag in tag_list:
                        if len(tag) > 1:
                            if tag[0] == '#':
                                tmp_tags_.append(tag[1:])
                    for tag in tmp_tags_:
                        self.tags[tag] -= 1
                    #self.tags[tmp_tags] = 0
                    self.tweet.append(words)
                    for tag in tmp_tags:
                        self.tags[tag] += 1

                    req = []
                    for i in list(self.tags.items()):
                        req.append(i[1])
                    req = list(set(req))
                    req_ = sorted(req, reverse = True)[:3]
                    fin = []
                    for i in list(self.tags.items()):
                        if i[1] in req_:
                            fin.append(i)
                    fin = sorted(fin, key=lambda x:(-1*x[1],x[0]))
                    with open(output_file, 'a') as f:
                        f.write(
                            'The number of tweets with tags from the begining: '+ str(self.cnt) + '\n')
                        for item in fin:
                            f.write(str(item[0]) + ':' + str(item[1]) + '\n')
                        f.write('\n')
                    
                    

accessToken = '1324237515709063168-FTCBW7CXcTB0grRalJOEC9DHIvGIew'
accessSecretKey = 'tkRDBjFLzAD2oyLT8ASsMqPjvNS3ODEVn3y6wzSi5PGDJ'

auth = tweepy.OAuthHandler('L4igJsnGaycSiTGKpFbNaZFT3',
                           'pXcKTNndaBp07dfhV61wI0LjJCLDG7Tsebn1qBjavvjAU5ZJcA')
auth.set_access_token(key=accessToken, secret=accessSecretKey)
api = tweepy.API(auth, wait_on_rate_limit=True)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=auth, listener=myStreamListener)
myStream.filter(languages=['en'],track=['#'])