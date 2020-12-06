# Oxford Covid Data
This data is pulled from Oxford's covid policy tracker repository
- https://github.com/OxCGRT/covid-policy-tracker

- Specifically this one file:
  - https://github.com/OxCGRT/covid-policy-tracker/blob/master/data/OxCGRT_latest.csv

Please refer to their readme documents for in depth explanations on how their data is collected and used
___
### Oxford Data Directory
- OxCGRT_latest.csv (the original data pulled from their github)
- oxford_clean.csv (the original data formatted for our analysis purposes)
- run_all_jobs.sh
  
  used in the NYU Hadoop cluster to run all the calculation jobs
  
  note there are other scripts to run specific jobs
- calculations folder
  - daily changes (for each policy index, covid counts, and death counts)
  - cumulative log sums (calculated from daily changes)
  - total log sums