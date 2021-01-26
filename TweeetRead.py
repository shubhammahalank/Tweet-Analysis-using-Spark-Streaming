
import tweepy
from tweepy import OAuthHandler, Stream

from tweepy.streaming import StreamListener
import socket
import json

consumer_key = 'uZUaFEo4hkn5BUtNlZ8CvPjxN'
consumer_secret = 'm9oyBQLKDqUeba1XrSR5f23a1GISP0NFLlgg77YgRNtPOBhb9e'
access_token = '247702251-Syu0swI1ezG2xw5VfnSncqTb2bjhjUFr2XGPJqyG'
access_secret = '94zaWr6pRFjExaGKi3bnts9mL1GcZxMBC86h6BqDYwfVC'

class TweetListener(StreamListener):
    
    def __init__(self, csocket):
        self.client_socket = csocket
        
    def on_data(self, data):
        
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            # self.client_socket.send(msg['text'].encode('utf-8'))
            self.client_socket.send((str(msg['text']) + "\n").encode('utf-8'))
            return True
        
        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True
            
    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track = ['Tesla'])

if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 5554
    s.bind((host, port))
    
    print('Listening on port 5555')
    
    s.listen(5)
    c, addr = s.accept()
    
    sendData(c)




