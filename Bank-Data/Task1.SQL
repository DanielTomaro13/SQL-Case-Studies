Task 1: Analysing Bank Data 
We will be doing some analytics on real data from a Portuguese banking institution. The data
is stored in a semicolon (“;”) delimited format.

DROP TABLE bankdata;

-- Create a table for the input data
CREATE TABLE bankdata (age BIGINT, job STRING, marital STRING, education STRING,
  default STRING, balance BIGINT, housing STRING, loan STRING, contact STRING,
  day BIGINT, month STRING, duration BIGINT, campaign BIGINT, pdays BIGINT,
  previous BIGINT, poutcome STRING, termdeposit STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank-small.csv' INTO TABLE bankdata;

-- Report the number of clients for each marital status who have a balance above
-- 500 and has a loan.

SELECT 
    marital, 
    COUNT(*) as client_count
FROM 
    bankdata
WHERE 
    balance > 500 
    AND loan = 'yes'
GROUP BY 
    marital


INSERT OVERWRITE LOCAL DIRECTORY './Task_1a-out/'



