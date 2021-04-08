import json
import os

import psycopg2 as db
from dotenv import dotenv_values

from twitterimporter.twitterimporter import Tweet, TwitterImporter


def main():
    env = dotenv_values()
    root_folder = env["folder"]
    file_extension = ".txt"
    table_name = "tweets_final"

    conn = db.connect(
        **{
            _: env[_]
            for _ in env
            if _ in ["dbname", "user", "password", "host", "port"]
        }
    )
    importer = TwitterImporter(conn=conn)

    for i, (tweet_folder, _, __) in enumerate(os.walk(root_folder)):
        if i != 0:
            for f in os.listdir(tweet_folder):
                if f.endswith(file_extension) and not f.startswith("."):
                    file_name = f"{tweet_folder}/{f}"
                    with open(file_name) as fh:
                        print(f"Reading {f}...")
                        for line in fh.readlines():
                            tw_dict = json.loads(line)
                            if "user" in tw_dict.keys():
                                try:
                                    if (
                                        tw_dict["place"]["place_type"] == "poi"
                                    ):  # only import POI tweets
                                        tweet = Tweet(**tw_dict)
                                        importer.import_tweet(
                                            tweet=tweet, table_name=table_name
                                        )
                                except TypeError:  # some tweets don't have a place tag
                                    pass


if __name__ == "__main__":
    main()