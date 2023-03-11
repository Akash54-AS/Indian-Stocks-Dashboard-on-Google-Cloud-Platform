import tweepy

# Replace the placeholders with your own Twitter API credentials
consumer_key = "YOUR_CONSUMER_KEY"
consumer_secret = "YOUR_CONSUMER_SECRET"
access_token = "YOUR_ACCESS_TOKEN"
access_token_secret = "YOUR_ACCESS_TOKEN_SECRET"

# Authenticate with Twitter API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Define a stream listener that prints received tweets to console
class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)

# Create a stream object with the authenticated API and stream listener
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = auth, listener=myStreamListener)

# Filter the stream for tweets containing the keyword "python"
myStream.filter(track=['python'])
print(myStream)