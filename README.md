# SQL_Query_Weather_Breakdowns_in_Poland
Listing of sudden weather breakdowns in Poland in year 2000 <br>

Sudden weather breakdowns are defined as:
- previous observation without precipitation,
- precipitation greater than the rolling average for 7 observations
- temperature lower by at least 5 degrees than the average temperature for the last 4 observations

Source: BigQuery publicly available table: <br>
bigquery-public-data:noaa_gsod.gsod2000
