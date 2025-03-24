Task 2 
Report the average yearly balance for all people in each job category in
descending order of average yearly balance.

-- Create a table for the input data
CREATE TABLE bankdata (
  age BIGINT, 
  job STRING, 
  marital STRING, 
  education STRING,
  default STRING, 
  balance BIGINT, 
  housing STRING, 
  loan STRING, 
  contact STRING,
  day BIGINT, 
  month STRING, 
  duration BIGINT, 
  campaign BIGINT, 
  pdays BIGINT,
  previous BIGINT, 
  poutcome STRING, 
  termdeposit STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank.csv' INTO TABLE bankdata;

SELECT
    job,
    AVG(balance) AS average_yearly_balance
FROM
    bankdata
GROUP BY
    job
ORDER BY
    average_yearly_balance DESC