# Oxford Covid Data

This data is pulled from Oxford's covid policy tracker repository

- <https://github.com/OxCGRT/covid-policy-tracker>

- Specifically this one file:
  - <https://github.com/OxCGRT/covid-policy-tracker/blob/master/data/OxCGRT_latest.csv>

Please refer to their readme documents for in depth explanations on how their data is collected and used
___

## Oxford Data Directory

- OxCGRT_latest.csv

  the original data pulled from their github

- oxford_clean.csv

  the original data formatted for our analysis purposes

- calculations folder
  - oxford_daily_changes.csv

    daily changes for covid counts, death counts, and each policy index

  - oxford_index_cumulative_sums.csv
  
    cumulative log sums calculated from daily changes

  - oxford_index_total_sums
  
    total log sums for covid counts, death counts, and each policy index
