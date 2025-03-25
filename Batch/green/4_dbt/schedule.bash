# for both yellow and green 

Steps to Schedule a Job in dbt Cloud:
1- Go to dbt Cloud 
2- Open your dbt Cloud account.
3- create dbt environment for deployment  (e.g., Production).
4- Create a Job
5-dbt run --select green_full_trips
  dbt run --select yellow_full_trips
6- Schedule the Job(6 hr)
7- save
