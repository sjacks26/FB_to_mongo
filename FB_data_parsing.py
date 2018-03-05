'''
Author: Sam Jackson (sjackson@syr.edu)
This script opens a single json file containing the raw data returned by the FB Graph API.
It determine what kind of data the file contains based on the filename (i.e., page data, post data, comments data, or replies data).
It then parses that data according to the template as defined in the functions below.
The returned object for each of the different functions is a list of json objects that are ready to be uploaded to mongo.
Once the contents of the input file have been processed, they are simultaneously written to a new file
 (with the original filename plus "_processed" before the file extension) and uploaded to Mongo.
 The db they are written to is specified at the top of the script. The collection they are written to is determined by
  the type of file (i.e., pages, posts, and comments/replies)

Sample use:
process(test_file)
'''

import json
import os
from dateutil.parser import parse
import pymongo
from bson import json_util

mongoClient = pymongo.MongoClient()
db = mongoClient.test_March2018
"""
Need to add code for credentialed mongo.
"""

# candidate_names should be a dictionary. Keys are FB id integers, values are candidate names as we want them to appear in Mongo.
# It might make sense to get this info from somewhere else. I'm not sure.
candidate_names = {
    58736997707: "Marco Rubio"
}


def parse_page(self):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    page_data = []
    good_json = {
        'page_id': data['id'],
        'user_name': data['username'],
        'name': data['name'],
        'page_link': data['link'],
        'fan_count': data['fan_count'] if 'fan_count' in data else 0,
        'talking_about_count': data['talking_about_count'] if 'talking_about_count' in data else 0  # need a timestamp here
    }
    page_data.append(good_json)
    return page_data


def parse_post(self):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    cand_id = int(data['id'].split('_')[0])
    post_data = []
    good_json = {
        'candidate_name': candidate_names[cand_id],
        'post_id': data['id'],
        'created_time': data['created_time'],
        'created_ts': parse(data['created_time']),
        'message_text': data['message'] if 'message' in data else '',
        'comment_count': data['comments']['summary']['total_count'] if 'comments' in data else 0,
        'likes_count': data['likes']['summary']['total_count'] if 'likes' in data else 0,
        'share_count': data['shares']['count'] if 'shares' in data else 0,
        'updated_time': data['updated_time'],
        'updated_ts': parse(data['updated_time'])
    }
    post_data.append(good_json)
    return post_data


def parse_comments(self, filename):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    comment_list = []
    candidate_name = candidate_names[int(filename.split('_')[2])]
    for c in data['data']:
        good_c = {
            'comment_id': c['id'],
            'post_id': c['id'].split('_')[0],
            'candidate_name': candidate_name,
            'comment_like_count': c['like_count'] if 'like_count' in c else 0,
            'comment_text': c['message'],
            'created_time': c['created_time'],
            'created_ts': parse(c['created_time'])
        }
        comment_list.append(good_c)
    return comment_list


def parse_replies(self, filename):
    self = self.replace('\n', ' ')
    data = json.loads(self)
    replies_list = []
    reply_to = '_'.join(filename.split('_')[2:4])
    for r in data['data']:
        good_r = {
            'comment_id': r['id'],
            'post_id': r['id'].split('_')[0],
            #'candidate_name': ,                        # I'm not sure what the best way is to get the candidate name for this
            'reply_to': reply_to,
            'comment_link_count': r['like_count'] if 'like_count' in r else 0,
            'comment_text': r['message'],
            'created_time': r['created_time'],
            'created_ts': parse(r['created_time'])
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
        processed_data = parse_page(raw_data)
        insertDB = db.pages
    elif 'post' in filename and 'comments' not in filename:
        processed_data = parse_post(raw_data)
        insertDB = db.posts
    elif 'replies' in filename:
        processed_data = parse_replies(raw_data, filename=filename)
        insertDB = db.comments
    elif 'comments' in filename:
        processed_data = parse_comments(raw_data, filename=filename)
        insertDB = db.comments
    return processed_data, insertDB


def write_and_insert_processed_data(file):
    processed_data, insertDB = parse_file(file)
    new_filename = file.replace(".json", "_processed.json")
    with open(new_filename, 'w') as o:
        for l in processed_data:
            t = json.dumps(l, default=json_util.default)
            o.write(t + "\n")
    print("Wrote processed data to " + new_filename)
    insertDB.insert_many(processed_data)
    print("Inserted " + str(len(processed_data)) + " records to " + insertDB.full_name)


def process(file):
    write_and_insert_processed_data(file)

# The following is used to test

# test_file = ''
# process(test_file)

startdir = ''
file_list = os.listdir(startdir)
file_list = [startdir + f for f in file_list]

for f in file_list:
    if not "_processed" in f:
        process(f)