import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json



class TweetsListener(StreamListener):
    # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text']) 
       
          
            self.client_socket.send(str(data).encode('utf-8'))

            
       

            ## if tweet is longer than 140 characters
            # if "extended_tweet" in msg:

            #     # add at the end of each tweet "t_end"
            #     self.client_socket \
            #         .send(str(msg['extended_tweet']['full_text'] + "t_end") \
            #               .encode('utf-8'))
            #     print(msg['extended_tweet']['full_text'])
            # else:
            #     # add at the end of each tweet "t_end"
            #     self.client_socket \
            #         .send(str(msg['text'] + "t_end") \
            #               .encode('utf-8'))
            #     print(msg['text'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket, keyword):
    print('start sending data from Twitter to socket')
    # authentication based on the credentials
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    # start sending data from the Streaming API
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=keyword, languages=["en"])


if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"
    port = 5659
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(6)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword=['imran khan'])
