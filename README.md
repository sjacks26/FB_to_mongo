# FB_to_mongo

This code takes data from FB's Graph API and writes that data to mongo. 

It iterates through a specified directory, finding all files that are at least 30 minutes old. When it finishes processing all files at least 30 minutes old, it sleeps for 1 hour before resuming -- this means that the code can be run indefinitely, rather than being run each time there is new data to insert.    
Using the filename, it determines whether the data contains a page, a post, comments, or replies.  
It then parses the data according to a template made for each type of data.  
  
It takes processed data and does the following:  
1) It writes the processed data to a new file, putting that processed file in a processed directory  
2) It inserts the processed data to Mongo 
3) It moves the raw file to a raw archive directory 

## Installation and setup
1) Clone the code to your machine.  
2) Rename `config_template.py` to `config.py`.
3) Modify the parameters in `config.py`:
    * Put the proper path to the directory containing data in base_dirc. FB_to_mongo assumes that this directory will contain a directory called "download" that will contain subdirectories with data.
    * Specify the name you want for the file that will contain info about candidates and FB page ids in candidate_info_json_file.
    * Enter the credentials for Mongo into mongo_auth. If your instance of mongo is not password-protected, change "AUTH" to False.
    * Enter the name you want for the Mongo DB into mongo_auth. FB_to_mongo will automatically create collection names that will live inside the DB you name here.
4) Run `create_candidate_info_json.py`
5) Check the file named in candidate_info_json_file in `config.py`. Make any changes as appropriate. The string that appears after the colon in each line will be the value for a field called "name" in the documents in each collection.
6) Run the code using `sudo python3 FB_data_parsing.py >> insert.log 2>&1 &`. This code is meant to run perpetually. When it finishes processing data, it will sleep for one hour before looking for new data to process.


## Requirements

* Python3  
* [pymongo](https://api.mongodb.com/python/current/)
* [dateutil](https://dateutil.readthedocs.io/en/stable/)
