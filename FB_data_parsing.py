'''
This script opens a single json file containing the raw data returned by the FB Graph API.
It determine what kind of data the file contains based on the filename (i.e., page data, post data, comments data, or replies data).
It then parses that data according to the template as defined in the functions below.
The returned object for each of the different functions is a list of json objects that are ready to be uploaded to mongo.
Once the contents of the input file have been processed, they are simultaneously written to a new file
 (with the original filename plus "_processed" before the file extension) and uploaded to Mongo.
 The db they are written to is specified at the top of the script. The collection they are written to is determined by
  the type of file (i.e., pages, posts, and comments/replies)

'''

import json
import os
from dateutil.parser import parse
import pymongo
from bson import json_util
from pymongo import errors
import time
import config as cfg

mongoClient = pymongo.MongoClient()
if cfg.mongo_auth['AUTH']:
    mongoClient.admin.authenticate(cfg.mongo_auth['username'],cfg.mongo_auth['password'])
db = mongoClient[cfg.mongo_auth['db_name']]

base_dirc = cfg.base_dirc


def resolve_key_id(data):
    """
    For some reason, some FB data uses "page_id" and some uses "id" for the id number for the page it comes from.
     This function prevents errors stemming from this.
    """
    key_id = ''
    keys = list(data.keys())
    if "id" in keys:
        key_id = "id"
    elif "page_id" in keys:
        key_id = "page_id"
    return key_id


def get_candidate_names():
    candidate_info_json_file = cfg.candidate_info_json_file
    candidate_names = {}
    with open(candidate_info_json_file, 'r') as i:
        candidate_info = i.readlines()
    candidate_info = [f.strip() for f in candidate_info]
    for f in candidate_info:
        f = f.split(': ')
        candidate_names[int(f[0])] = f[1]

    return candidate_names


def parse_page(self, filename):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    page_data = []
    ts_from_filename = ' '.join([filename.split("_")[0], filename.split("_")[1].replace("-", ":")])
    tzinfos = {"UTC": +00000}
    key_id = resolve_key_id(data)
    good_json = {
        'page_id': int(data[key_id]),         # This gives a unique integer we can index on
        'user_name': data['username'],
        'name': data['name'],
        'page_link': data['link'],
        'fan_count': data['fan_count'] if 'fan_count' in data else 0,
        'talking_about_count': data['talking_about_count'] if 'talking_about_count' in data else 0,
        'updated_ts': parse(ts_from_filename, tzinfos=tzinfos)           # This pulls the timestamp from the filename
    }
    page_data.append(good_json)
    return page_data


def parse_post(self):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    key_id = resolve_key_id(data)
    cand_id = int(data[key_id].split('_')[0])
    post_data = []
    good_json = {
        'candidate_name': candidate_names[cand_id],
        'post_id': data[key_id],
        'post_id_int': int(data[key_id].split('_')[1]),         # This gives a unique integer we can index on
        'created_time': data['created_time'],
        'created_ts': parse(data['created_time']),
        'message_text': data['message'] if 'message' in data else '',
        'comment_count': data['comments']['summary']['total_count'] if 'comments' in data else 0,
        'like_count': data['likes']['summary']['total_count'] if 'likes' in data else 0,
        'share_count': data['shares']['count'] if 'shares' in data else 0,
        'updated_time': data['updated_time'],
        'updated_ts': parse(data['updated_time'])
    }
    post_data.append(good_json)
    return post_data


def parse_comments(self, filename):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    ts_from_filename = ' '.join([filename.split("_")[0], filename.split("_")[1].replace("-", ":")])
    tzinfos = {"UTC": +00000}
    comment_list = []
    candidate_name = candidate_names[int(filename.split('_')[2])]
    key_id = resolve_key_id(data)
    for c in data['data']:
        good_c = {
            'comment_id': c[key_id],
            'post_id': int(c[key_id].split('_')[0]),
            'comment_id_int': int(c[key_id].split('_')[1]),         # This gives a unique integer we can index on
            'candidate_name': candidate_name,
            'comment_like_count': c['like_count'] if 'like_count' in c else 0,
            'comment_text': c['message'],
            'created_time': c['created_time'],
            'created_ts': parse(c['created_time']),
            'updated_ts': parse(ts_from_filename, tzinfos=tzinfos)  # This pulls the timestamp from the filename)
        }
        comment_list.append(good_c)
    return comment_list


def parse_replies(self, filename):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    ts_from_filename = ' '.join([filename.split("_")[0], filename.split("_")[1].replace("-", ":")])
    tzinfos = {"UTC": +00000}
    replies_list = []
    reply_to = '_'.join(filename.split('_')[2:4])
    key_id = resolve_key_id(data)
    for r in data['data']:
        good_r = {
            'comment_id': r[key_id],
            'post_id': int(r[key_id].split('_')[0]),
            'comment_id_int': int(r[key_id].split('_')[1]),         # This gives a unique integer we can index on
            #'candidate_name': ,                        # I'm not sure what the best way is to get the candidate name for this
            'reply_to': reply_to,
            'comment_like_count': r['like_count'] if 'like_count' in r else 0,
            'comment_text': r['message'],
            'created_time': r['created_time'],
            'created_ts': parse(r['created_time']),
            'updated_ts': parse(ts_from_filename, tzinfos=tzinfos)  # This pulls the timestamp from the filename)
        }
        replies_list.append(good_r)
    return replies_list


def parse_file(file):
    """
    file should be the json file to be parsed. It doesn't matter whether it's an abspath or just the name of a file in the cwd.
    processed_data is a list of json objects that are ready to be inserted into mongo
    """
    with open(file) as f:
        raw_data = f.read()
    filename = os.path.basename(file)
    if 'page' in filename:
        processed_data = parse_page(raw_data, filename=filename)
        insertDB = db.FB_cand_page_crawl_history
        insert = insert_page
    elif 'post' in filename and 'comments' not in filename:
        processed_data = parse_post(raw_data)
        insertDB = (db.FB_cand_post, db.FB_cand_post_crawl_history)
        insert = insert_post
    elif 'replies' in filename:
        processed_data = parse_replies(raw_data, filename=filename)
        insertDB = db.FB_public
        insert = insert_replies
    elif 'comments' in filename:
        processed_data = parse_comments(raw_data, filename=filename)
        insertDB = db.FB_public
        insert = insert_comments
    return processed_data, insertDB, insert


def insert_page(processed_data, insertDB):
    insertDB.insert_many(processed_data)


def insert_post(processed_data, insertDB, historyDB):
    for post in processed_data:
        historyDB.insert_one(post)
        isExist = insertDB.find_one({'post_id': post['post_id']})
        if isExist is None:
            insertDB.insert_one(post)
        else:
            insertDB.update_one({'post_id': post['post_id']},
                            {'$set': {'comment_count': post['comment_count'],
                                       'like_count': post['like_count'],
                                       'share_count': post['share_count'],
                                       'updated_time': post['updated_time'],
                                       'updated_ts': post['updated_ts']}}, upsert=True)


def insert_replies(processed_data, insertDB):
    for reply in processed_data:
        isExist = insertDB.find_one({'comment_id': reply['comment_id']})
        if isExist is None:
            insertDB.insert_one(reply)
        else:
            insertDB.update_one({'comment_id': reply['comment_id']},
                             {'$set': {'comment_like_count': reply['comment_like_count'],
                                       'updated_ts': reply['updated_ts']}}, upsert=True)


def insert_comments(processed_data, insertDB):
    for comment in processed_data:
        isExist = insertDB.find_one({'comment_id': comment['comment_id']})
        if isExist is None:
            insertDB.insert_one(comment)
        else:
            insertDB.update_one({'comment_id': comment['comment_id']},
                             {'$set': {'comment_like_count': comment['comment_like_count'],
                                       'updated_ts': comment['updated_ts']}}, upsert=True)


def write_and_insert_processed_data(file, root_dirc):
    processed_data, insertDB, insert = parse_file(file)
    new_filename = file.replace(".json", "_processed.json")
    new_filename = new_filename.replace(os.path.dirname(file), os.path.join(root_dirc, "processed"))
    with open(new_filename, 'w') as o:
        for l in processed_data:
            t = json.dumps(l, default=json_util.default)
            o.write(t + "\n")
    print("Wrote processed data to " + new_filename)
    try:
        insert(processed_data, insertDB)
        print("Inserted " + str(len(processed_data)) + " records to " + insertDB.full_name)
    except TypeError:
        insert(processed_data, insertDB[0], insertDB[1])
        print("Inserted " + str(len(processed_data)) + " records to " + insertDB[1].full_name)

  
def process(root_dirc):
    dirc = os.path.join(root_dirc, "download")
    for root, dirs, files in os.walk(dirc):
        for file in files:
            filename = os.path.join(root,file)
            t = time.time() - 30 * 60
            if os.path.getmtime(filename) < t and not 'processed' in filename:
                write_and_insert_processed_data(filename, root_dirc)
                raw_filename = os.path.join(root_dirc,"raw",file)
                os.rename(filename, raw_filename)
               
               
def run_timeline(base_dirc):
    while True:
        os.makedirs(os.path.join(base_dirc,"raw"), exist_ok=True)
        os.makedirs(os.path.join(base_dirc, "processed"), exist_ok=True)
        process(base_dirc)
        print('Job completed. Resuming in an hour.')
        time.sleep(60 * 60)


candidate_names = get_candidate_names()
run_timeline(base_dirc)
