# How to recognize types of a tweet via its JSON data?
There are some key field in the tweet JSON data to help us understand the
purpose of this tweet.

  - `retweeted_status`, `tweet` object
  - `quoted_status`, `tweet` object
  - `in_reply_to_status_id`, `int64`, a pointer to tweet object

(1) If none of these fields is populated (not null), then this is a normal
tweet or original tweet. It happends when you write some text and post it
on twitter.

(2) If retweeted_status is populated, then this is a retweet. It happends when
you click retweet button to repost other's tweets without comment.

(3) If in_reply_to_status_id is populated, then this is a reply tweet. It
happends when you click reply button and reply to other's tweets.

(4) If quoted_status is populated **AND other two fields are not populated**,
then this is a quoted tweet. It happends when you retweet other's tweets
with comment, **OR you use the URL of other's status in your post**.
