import psycopg2 as db
from dotenv import dotenv_values
import tweepy

from twitterimporter.twitterimporter import Tweet, TwitterImporter


def main(debug=True):
    if debug:
        select_query = "select setseed(0.5); select user_id from london_tweets l where geometry is not null AND  NOT EXISTS (SELECT FROM timeline_tweets WHERE user_id = l.user_id ) group by user_id order by random() limit 200"
    else:
        select_query = "select user_id from london_tweets l where geometry is not null AND  NOT EXISTS (SELECT FROM timeline_tweets WHERE  user_id = l.user_id ) group by user_id"

    env = dotenv_values()
    conn = db.connect(
        **{
            _: env[_]
            for _ in env
            if _ in ["dbname", "user", "password", "host", "port"]
        }
    )

    iter_conn = db.connect(
        **{
            _: env[_]
            for _ in env
            if _ in ["dbname", "user", "password", "host", "port"]
        }
    )
    iter_cur = iter_conn.cursor()

    importer = TwitterImporter(conn=conn)
    table_name = "timeline_tweets"

    auth = tweepy.AppAuthHandler(env["api_key"], env["api_secret"])
    api = tweepy.API(auth)

    iter_cur.execute(select_query)

    while True:
        row = iter_cur.fetchone()
        if row == None:
            break
        user_id = row[0]
        tweets = tweepy.Cursor(
            api.user_timeline, id=user_id, tweet_mode="extended"
        ).items()

        for t in tweets:
            if "user" in t._json and t._json["place"] is not None:
                tweet = Tweet(**t._json)
                importer.import_tweet(tweet=tweet, table_name=table_name)


if __name__ == "__main__":
    main()