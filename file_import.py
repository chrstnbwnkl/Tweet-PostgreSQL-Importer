import json
import os

import psycopg2 as db
from dotenv import dotenv_values

from twitterimporter.twitterimporter import Tweet, TwitterImporter


def main():
    env = dotenv_values()
    tweet_folder = env["folder"]
    file_extension = ".txt"
    table_name = "london_tweets"

    conn = db.connect(
        **{
            _: env[_]
            for _ in env
            if _ in ["dbname", "user", "password", "host", "port"]
        }
    )
    importer = TwitterImporter(conn=conn)

    for i, f in enumerate(os.listdir(tweet_folder)):
        if f.endswith(file_extension):
            file_name = f"{tweet_folder}/{f}"
            with open(file_name) as fh:
                print(f"Reading {f}...")
                for line in fh.readlines():
                    tw_dict = json.loads(line)
                    if "user" in tw_dict.keys():
                        tweet = Tweet(**tw_dict)
                        importer.import_tweet(tweet=tweet, table_name=table_name)


if __name__ == "__main__":
    main()