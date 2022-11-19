import os
import sched
import time
import logging
from typing import List

import sqlalchemy as db
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)


class OsuApi:
    def __init__(self, client_id, client_secret):
        osu_client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=osu_client)
        self.token = oauth.fetch_token(token_url='https://osu.ppy.sh/oauth/token', client_id=client_id,
                                       client_secret=client_secret, scope=["public"])

        self.client = OAuth2Session(client_id, token=self.token)
        self.last_request_time = time.time()

    def get_user(self, osu_id: int):
        return self.get_endpoint(f"https://osu.ppy.sh/api/v2/users/{osu_id}/osu")

    def get_endpoint(self, url):
        time_since_last_req = time.time() - self.last_request_time
        if time_since_last_req < 1.5:
            time.sleep(1.5 - time_since_last_req)
        self.last_request_time = time.time()
        return self.client.get(url).json()


def update_users():
    start_time = time.perf_counter()

    client_id = os.getenv("OSU_CLIENT_ID")
    client_secret = os.getenv("OSU_CLIENT_SECRET")
    osu_client = OsuApi(client_id=client_id, client_secret=client_secret)

    conn, db_table_users = init_db()
    all_users = db_get_users(conn, db_table_users)

    column_names: List = db_table_users.columns.keys()

    for user in all_users:
        loop_start_time = time.perf_counter()
        osu_id = user[column_names.index("osu_id")]
        old_osu_username = user[column_names.index("osu_username")]
        old_dc_username = user[column_names.index("discord_tag")]
        user_badges = user[column_names.index("badges")]

        user_details = osu_client.get_user(osu_id)
        if 'error' in user_details:
            logging.info(f"Errored for: {osu_id} - {old_osu_username} & {old_dc_username}")
            delete_query = db_table_users.delete().where(db_table_users.columns.osu_id == osu_id)
            conn.execute(delete_query)
            continue

        new_osu_username = user_details["username"]
        global_rank = user_details["statistics"]["global_rank"]
        if global_rank is None:
            global_rank = 0

        bws_rank = round(global_rank ** (0.9937 ** (user_badges ** 2)))

        update_query = db_table_users.update().values(osu_global_rank=global_rank, bws_rank=bws_rank,
                                                      osu_username=new_osu_username).where(
            db_table_users.columns.osu_id == osu_id)
        conn.execute(update_query)
        logging.info(f"Updated a single user in: {time.perf_counter() - loop_start_time:.3f}s")

    logging.info(f"Updated all users in: {time.perf_counter() - start_time:.3f}s")


def db_get_users(conn, db_table_users):
    query = db_table_users.select()
    exe = conn.execute(query)
    results = exe.fetchall()
    return results


def init_db():
    engine = db.create_engine(os.getenv("DATABASE_URL"))
    conn = engine.connect()
    metadata = db.MetaData()  # extracting the metadata
    db_table_users = db.Table('users', metadata, autoload=True,
                              autoload_with=engine)  # Table object
    return conn, db_table_users


if __name__ == '__main__':
    s = sched.scheduler()
    while True:
        s.enter(900, 1, update_users)
        s.run()
