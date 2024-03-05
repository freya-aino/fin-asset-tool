
# def fetch_poll(poll_hn_id):

#     cur = hn_db.cursor()
#     cur.execute("select poll_id from poll where hn_id = " + str(poll_hn_id))
#     rows = cur.fetchall()

#     if len(rows) > 1:
#         raise Exception("hacker news key duplicate when fetching poll")
#     elif len(rows) == 1:
#         return rows[0][0]
#     else:
        
#         fetch_and_insert_recursively(poll_hn_id)

#         cur = hn_db.cursor()
#         cur.execute("select poll_id from poll where hn_id = " + str(poll_hn_id))
#         rows = cur.fetchall()

#         assert len(rows) == 1, "more than one unique row or 0 rows on hn_id global unique key"
#         return rows[0][0]

# def fetch_child(child_hn_id):
    
#     cur = hn_db.cursor()
#     cur.execute("select comment_id from comment where hn_id = " + str(child_hn_id))
#     rows = cur.fetchall()

#     if len(rows) > 1:
#         raise Exception("hacker news key duplicate when fetching child")
#     elif len(rows) == 1:
#         return rows[0][0]
#     else:

#         fetch_and_insert_recursively(child_hn_id)
        
#         cur = hn_db.cursor()
#         cur.execute("select comment_id from comment where hn_id = " + str(child_hn_id))
#         comment_rows = cur.fetchall()

#         assert len(comment_rows) > 0, "child still has no reference"
        
#         return comment_rows[0][0]

# def fetch_comment_parent(parent_hn_id):

#     cur = hn_db.cursor()
#     cur.execute("select comment_id from comment where hn_id = " + str(parent_hn_id))
#     comment_rows = cur.fetchall()
#     cur.execute("select story_id from story where hn_id = " + str(parent_hn_id))
#     story_rows = cur.fetchall()

#     assert len(comment_rows) == 0 or len(story_rows) == 0, "parent referes to both a comment and a story"

#     if len(comment_rows) > 0:
#         return comment_rows[0][0], "comment"
#     elif len(story_rows) > 0:
#         return story_rows[0][0], "story"
#     else:

#         fetch_and_insert_recursively(parent_hn_id)
        
#         cur = hn_db.cursor()
#         cur.execute("select comment_id from comment where hn_id = " + str(parent_hn_id))
#         comment_rows = cur.fetchall()
#         cur.execute("select story_id from story where hn_id = " + str(parent_hn_id))
#         story_rows = cur.fetchall()

#         assert len(comment_rows) == 0 or len(story_rows) == 0, "parent referes to both a comment and a story"
#         assert (len(comment_rows) + len(story_rows)) > 0, "parent still have no reference"
        
#         if len(comment_rows) > 0:
#             return comment_rows[0][0], "comment"
#         else:
#             return story_rows[0][0], "story"

# def fetch_poll_opt(poll_opt_hn_id):

#     cur = hn_db.cursor()
#     cur.execute("select poll_opt_id from poll where hn_id = " + str(poll_opt_hn_id))
#     rows = cur.fetchall()

#     if len(rows) > 1:
#         raise Exception("hacker news key duplicate when fetching poll opt")
#     elif len(rows) == 1:
#         return rows[0][0]
#     else:
        
#         fetch_and_insert_recursively(poll_opt_hn_id)

#         cur = hn_db.cursor()
#         cur.execute("select poll_opt_id from poll where hn_id = " + str(poll_opt_hn_id))
#         rows = cur.fetchall()

#         assert len(rows) == 1, "more than one unique row or 0 rows on hn_id global unique key"
#         return rows[0][0]

# # ------------------------------------------------------

# def fetch_and_insert_recursively(hn_id):

#     item = get("https://hacker-news.firebaseio.com/v0/item/"+str(hn_id)+".json")
#     assert item.status_code >= 200 and item.status_code < 300, "item, non valid response!"
#     item = json.loads(item.content)

#     type_ = item.get("type", None)

#     logging.info("start fetching\t" + str(type_) + " " + str(hn_id))

#     sleep(0.5)

#     if type_ == "job": # no recursive calls

#         id            = item.get("id", None)
#         author        = item.get("by", None)
#         is_deleted    = 1 if item.get("deleted", 0) != 0 else 0
#         is_dead       = 1 if item.get("dead", 0) != 0 else 0
#         time          = item.get("time", None)
#         title         = item.get("title", None)
#         text          = item.get("text", None)
#         score         = item.get("score", None)

#         sql_str = "insert into job (hn_id, author, is_deleted, is_dead, dt_created, title, text, score) values (?, ?, ?, ?, ?, ?, ?, ?, ?);"
#         sql_params = (id, author, is_deleted, is_dead, time, title, text, score)

#         hn_db.execute(sql_str, sql_params)
#         hn_db.commit()

#         logging.info("comitted       \t" + type_ + " " + str(hn_id))

#     elif type_ == "pollopt":

#         id               = item.get("id", None)

#         id_exists = hn_db.execute("select exists(select 1 from poll_opt where hn_id = " + str(id) + ")").fetchone()[0]
#         if not id_exists:
#             logging.info("initializing   \t" + type_ + " " + str(id))
#             hn_db.execute("insert into poll_opt (hn_id) values (" + str(id) + ");")
#             hn_db.commit()

#         author           = item.get("by", None)
#         is_deleted       = 1 if item.get("deleted", 0) != 0 else 0
#         is_dead          = 1 if item.get("dead", 0) != 0 else 0
#         time             = item.get("time", None)
#         text             = item.get("text", None)
#         score            = item.get("score", None)
#         related_pollopts = item.get("related_pollopts", None)
#         poll_id          = item.get("poll_id", None)

#         if poll_id != None:
#             poll_id = fetch_poll(poll_id)

#         sql_str = "".join(["update poll_opt", 
#                         "\nset author = ?,", 
#                         "\n\tis_deleted = ?,",
#                         "\n\tis_dead = ?,",
#                         "\n\tdt_created = ?,",
#                         "\n\ttext = ?,",
#                         "\n\tpoll_id = ?,",
#                         "\n\tscore = ?,",
#                         "\n\trelated_pollopts = ?",
#                         "\nwhere hn_id = ?;"])

#         sql_params = (author, is_deleted, is_dead, time, text, poll_id, score, json.dumps(related_pollopts), id)

#         hn_db.execute(sql_str, sql_params)
#         hn_db.commit()

#         logging.info("comitted        \t" + type_ + " " + str(hn_id))

#     elif type_ == "comment":

#         id                     = item.get("id", None)
        
#         id_exists = hn_db.execute("select exists(select 1 from comment where hn_id = " + str(id) + ")").fetchone()[0]
#         if not id_exists:
#             logging.info("initializing   \t" + type_ + " " + str(id))
#             hn_db.execute("insert into comment (hn_id) values (" + str(id) + ");")
#             hn_db.commit()

#         author                 = item.get("by", None)
#         is_deleted             = 1 if "deleted" in item else 0
#         is_dead                = 1 if "dead" in item else 0
#         text                   = item.get("text", None)
#         time                   = item.get("time", None)
#         children               = item.get("kids", [])
#         parent_id, parent_type = fetch_comment_parent(item["parent"])

#         if parent_type == "story":
#             parent_comment_id = None
#             parent_story_id = parent_id
#         else:
#             parent_comment_id = parent_id
#             parent_story_id = None

#         for i in range(len(children)):
#             children[i] = fetch_child(children[i])

#         sql_str = "".join(["update comment", 
#                             "\nset author = ?,", 
#                             "\n\tis_deleted = ?,",
#                             "\n\tis_dead = ?,", 
#                             "\n\tdt_created = ?,", 
#                             "\n\tparent_comment_id = ?,",
#                             "\n\tparent_story_id = ?,", 
#                             "\n\ttext = ?,", 
#                             "\n\tchildren = ?", 
#                             "\nwhere hn_id = ?;"])

#         sql_params = (author, is_deleted, is_dead, time, parent_comment_id, parent_story_id, text, json.dumps(children), id)

#         hn_db.execute(sql_str, sql_params)
#         hn_db.commit()

#         logging.info("comitted       \t" + type_ + " " + str(hn_id))

#     elif type_ == "story":

#         id                     = item.get("id", None)
        
#         id_exists = hn_db.execute("select exists(select 1 from story where hn_id = " + str(id) + ")").fetchone()[0]
#         if not id_exists:
#             logging.info("initializing   \t" + type_ + " " + str(id))
#             hn_db.execute("insert into story (hn_id) values (" + str(id) + ");")
#             hn_db.commit()

#         author                 = item.get("by", None)
#         is_deleted             = 1 if "deleted" in item else 0
#         is_dead                = 1 if "dead" in item else 0
#         descendants            = item.get("descendants", None)
#         score                  = item.get("score", None)
#         title                  = item.get("title", None)
#         text                   = item.get("text", None)
#         time                   = item.get("time", None)
#         children               = item.get("kids", [])

#         for i in range(len(children)):
#             children[i] = fetch_child(children[i])

#         sql_str = "".join(["update story",
#                         "\nset author = ?,",
#                         "\n\tis_deleted = ?,",
#                         "\n\tis_dead = ?,",
#                         "\n\tdt_created = ?,",
#                         "\n\ttext = ?,",
#                         "\n\ttitle = ?,",
#                         "\n\tnumber_descendants = ?,",
#                         "\n\tscore = ?,",
#                         "\n\tchildren = ?",
#                         "\nwhere hn_id = ?;"])

#         sql_params = (author, is_deleted, is_dead, time, text, title, descendants, score, json.dumps(children), id)

#         hn_db.execute(sql_str, sql_params)
#         hn_db.commit()
        
#         logging.info("comitted       \t" + type_ + " " + str(hn_id))
        
#     elif type_ == "poll":
            
#         id          = item.get("id", None)
        
#         id_exists = hn_db.execute("select exists(select 1 from poll where hn_id = " + str(id) + ")").fetchone()[0]
#         if not id_exists:
#             logging.info("initializing   \t" + type_ + " " + str(id))
#             hn_db.execute("insert into poll (hn_id) values (" + str(id) + ");")
#             hn_db.commit()

#         author      = item.get("by", None)
#         is_deleted  = 1 if "deleted" in item else 0
#         is_dead     = 1 if "dead" in item else 0
#         descendants = item.get("descendants", None)
#         score       = item.get("score", None)
#         title       = item.get("title", None)
#         text        = item.get("text", None)
#         time        = item.get("time", None)
#         children    = item.get("kids", [])
#         poll_opts   = item.get("parts", [])

#         for i in range(len(poll_opts)):
#             poll_opts[i] = fetch_poll_opt(poll_opts[i])

#         for i in range(len(children)):
#             children[i] = fetch_child(children[i])

#         sql_str = "".join(["update poll", 
#                         "\nset author = ?",
#                         "\n\tis_deleted = ?",
#                         "\n\tis_dead = ?,",
#                         "\n\tdt_created = ?,",
#                         "\n\ttext = ?,",
#                         "\n\ttitle = ?,",
#                         "\n\tnumber_descendants = ?,",
#                         "\n\tscore = ?,",
#                         "\n\tchildren = ?,",
#                         "\n\tpoll_opts = ?",
#                         "\nwhere hn_id = ?;"])
                        
#         sql_params = (author, is_deleted, is_dead, time, text, title, descendants, score, json.dumps(children), poll_opts, id)

#         hn_db.execute(sql_str, sql_params)
#         hn_db.commit()

#         logging.info("comitted       \t" + type_ + " " + str(hn_id))

#     else:

#         logging.warning("had to insert into misk category (type: " + str(item.get("type", "not given") + ")"))

#         hn_db.execute("insert into misk (hn_id, data)",
#                      (item["id"], item))
#         hn_db.commit()

#         logging.info("comitted       \t" + type_ + " " + str(hn_id))


# TODO