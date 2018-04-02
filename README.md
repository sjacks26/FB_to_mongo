# FB_to_mongo

This code takes data from FB's Graph API and writes that data to mongo.  
  
The script requires a start directory path (line 230). It iterates through that directory, finding all files that are at least 30 minutes old. When it finishes processing all files at least 30 minutes old, it sleeps for 1 hour before resuming -- this means that the code can be run indefinitely, rather than being run each time there is new data to insert.  
Using the filename, it determines whether the data contains a page, a post, comments, or replies.  
It then parses the data according to a template made for each type of data.  
  
It takes processed data and does the following:  
1) It writes the processed data to a new file, putting that processed file in a processed directory  
2) It inserts the processed data to Mongo 
3) It moves the raw file to a raw archive directory

The Mongo db name is specified in line 25. The collection name is dependent on the type of data (page, post, comment/reply).

If your instance of MongoDB is password-protected, add the username and password to line 22
