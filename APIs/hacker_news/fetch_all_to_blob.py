from requests import get
from time import time
from pathlib import Path
import json
import sqlite3
import logging

# -------------------------------------------------------------

# possible API return fields

# id	The item\'s unique id.
# deleted	true if the item is deleted.
# type	The type of item. One of "job", "story", "comment", "poll", or "pollopt".
# by	The username of the item\'s author.
# time	Creation date of the item, in Unix Time.
# text	The comment, story or poll text. HTML.
# dead	true if the item is dead.
# parent	The comment\'s parent: either another comment or the relevant story.
# poll	The pollopt\'s associated poll.
# kids	The ids of the item\'s comments, in ranked display order.
# url	The URL of the story.
# score	The story\'s score, or the votes for a pollopt.
# title	The title of the story, poll or job. HTML.
# parts	A list of related pollopts, in display order.
# descendants	In the case of stories or polls, the total comment count.

# -----------------------------------------------------

def fetch_into_blob(hn_id, hn_db):

    logging.info("fetching  \t" + str(hn_id))

    item = get("https://hacker-news.firebaseio.com/v0/item/"+str(hn_id)+".json")
    assert item.status_code >= 200 and item.status_code < 300, "item, non valid response!"
    item = json.loads(item.content)

    if item != None:

        hn_id                   = item.get("id", None)
        is_deleted              = 1 if "deleted" in item else 0
        is_dead                 = 1 if "dead" in item else 0
        type_                   = item.get("type", 0)
        author                  = item.get("by", None)
        dt_source_created       = item.get("time", None)
        dt_inserted             = time() 
        title                   = item.get("title", None)
        text                    = item.get("text", None)
        parent_hn_id            = item.get("parent", None)
        poll_hn_id              = item.get("poll", None)
        score                   = item.get("score", None)
        poll_opt_hn_id_list     = item.get("parts", [])
        children_hn_id_list     = item.get("kids", None)
        descendants_hn_id_list  = item.get("descendants", None)

        sql_str = "".join(["insert or ignore into blob (",
                        "hn_id,",
                        "is_deleted,",
                        "is_dead,",
                        "type,",
                        "author,",
                        "dt_source_created,",
                        "dt_load,",
                        "title,",
                        "text,",
                        "parent_hn_id,",
                        "poll_hn_id,",
                        "score,",
                        "poll_opt_hn_id_list,",
                        "children_hn_id_list,",
                        "descendants_hn_id_list)",
                        "\nvalues (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"])
                        
        sql_params = (hn_id, is_deleted, is_dead, type_, author, dt_source_created, dt_inserted, 
                    title, text, parent_hn_id, poll_hn_id, score, 
                    json.dumps(poll_opt_hn_id_list), json.dumps(children_hn_id_list), json.dumps(descendants_hn_id_list))

        hn_db.execute(sql_str, sql_params)
        hn_db.commit()

        logging.info("done    \t" + str(hn_id))
    else:
        logging.warning("item is None")

# ------------------------------------------------------

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    hn_db = sqlite3.connect("C:\\datasets\\hacker_news.db")


    all_current_hn_ids = [a[0] for a in hn_db.execute("select hn_id from blob;").fetchall()]

    source_highest = get("https://hacker-news.firebaseio.com/v0/maxitem.json")
    
    assert source_highest.status_code >= 200 and source_highest.status_code < 300, "item, non valid response!"
    
    source_highest = json.loads(source_highest.content)

    
    for i in range(max(all_current_hn_ids) + 1, source_highest):
        dt = time()
        fetch_into_blob(i, hn_db)
        logging.info("fetch newest: elapsed time: " + str(((time() - dt) * (source_highest - i)) / 60))

    for i in range(min(all_current_hn_ids), 0, -1):
        dt = time()
        fetch_into_blob(i, hn_db)
        logging.info("fetch older: elapsed time: " + str(((time() - dt) * i) / 60))