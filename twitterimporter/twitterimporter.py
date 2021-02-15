from datetime import datetime
from psycopg2 import sql


class TwitterImporter:
    def __init__(self, conn=None):
        self._conn = conn
        self._cur = conn.cursor()

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, value):
        self._conn = value

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, value):
        self._mode = value

    def import_tweet(self, tweet, table_name):
        query_template = sql.SQL(
            "INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s) ON CONFLICT DO NOTHING"
        ).format(sql.Identifier(table_name))
        query = self._cur.mogrify(query_template, tweet.to_tuple())
        self._cur.execute(query)
        self._conn.commit()


class Tweet:
    def __init__(self, *args, **kwargs):
        if "created_at" in kwargs.keys():
            setattr(self, "created_at", self._to_datetime(kwargs["created_at"]))

        for k in ("id_str", "lang", "retweeted"):  # basic tweet info
            if k in kwargs:
                setattr(self, k, kwargs[k])

        for j in ("id_str", "description", "location", "geo_enabled"):  # user info
            if j in kwargs["user"]:
                setattr(self, f"user_{j}", kwargs["user"][j])

        if "extended_tweet" in kwargs.keys():
            setattr(self, "text", kwargs["extended_tweet"]["full_text"])

        elif "text" in kwargs.keys():
            setattr(self, "text", kwargs["text"])

        elif "full_text" in kwargs.keys():
            setattr(self, "text", kwargs["full_text"])

        if kwargs["coordinates"] is not None:
            for idx, coord in enumerate(["lon", "lat"]):
                setattr(self, coord, kwargs["coordinates"]["coordinates"][idx])
        else:
            for coord in ["lon", "lat"]:
                setattr(self, coord, None)

        if "place" in kwargs.keys() and kwargs["place"] is not None:
            for p in ["full_name", "id"]:
                setattr(self, f"place_{p}", kwargs["place"][p])
        else:
            for p in ["full_name", "id"]:
                setattr(self, f"place_{p}", None)

    def _to_datetime(self, dtime):
        new_datetime = datetime.strptime(dtime, "%a %b %d %H:%M:%S +0000 %Y")

        return new_datetime

    def to_tuple(self):
        return tuple(self.__dict__.values())