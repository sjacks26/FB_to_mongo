# FB_to_mongo

This code reads in an individual file containing data from FB's Graph API.  
  
Using the filename, it determines whether the data contains a page, a post, comments, or replies.  
It then parses the data according to a template made for each type of data.  
  
It takes processed data and does two things:  
1) It writes the processed data to a new file  
2) It inserts the processed data to Mongo.  

The Mongo db name is specified in line 28. The collection name is dependent on the type of data (page, post, comment/reply).

If your instance of MongoDB is password-protected, add the username and password to line 25
