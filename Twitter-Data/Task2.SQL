Task 2
Find the hash tag name that
was tweeted the most in the entire data set across all months. Report the total number
of tweets for that hash tag name. 

DROP TABLE twitterdata;

-- Create a table for the input data
CREATE TABLE twitterdata (
  tokenType STRING, 
  month STRING, 
  count BIGINT,
  hashtagName STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/twitter.tsv' INTO TABLE twitterdata;

SELECT 
    hashtagName, 
    SUM(count) as total_count
FROM 
    twitterdata
GROUP BY 
    hashtagName
ORDER BY 
    total_count DESC
LIMIT 1

INSERT OVERWRITE LOCAL DIRECTORY './Task_2b-out/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '

