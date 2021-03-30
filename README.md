# bike-weather-analytics
A fictionary analytics application created based of New York City Bike and Weather Data on Snowflake.

## Pre-requisites
- Snowflake account
- user with AccountAdmin or SecurityAdmin role
- snowsql installation
- Sample data files access in S3, if required

## Usage (MacOS / Linux)
### Step1: Set Snowflake environment variables
  export SNOW_ACCOUNT=
  export SNOW_REGION=
  export SNOW_PWD=
	export SNOW_USER=

### Step2 : Invoke launcher script
python3 src/scripts/bike-analytics-launcher.py

## Result
Expect to see the following Tables and Views after successful run of this application.

![Alt text](static/bike_analytics_catalog.png?raw=true "Analytics Catalog")


## Credits: 
https://docs.snowflake.net/manuals/user-guide-getting-started.html
https://bit.ly/2JJZl3J

## Reference : 
https://s3.amazonaws.com/snowflake-workshop-lab/InpersonZTS_LabGuide.pdf

## Author: https://github.com/sudheerpalyam