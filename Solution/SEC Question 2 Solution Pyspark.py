import datetime, calendar, pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Read historical averages data from CSV file
historical_avg = pd.read_csv("/Users/anveshmalireddy/Downloads/historical_averages.csv", sep=",", keep_default_na=False)

# Read silo actuals data from CSV file
silo_actuals = pd.read_csv("/Users/anveshmalireddy/Downloads/silo_actuals.csv", sep=",", keep_default_na=False)

# Reset index of historical averages DataFrame
historical_avg_with_index = historical_avg.reset_index()

historical_avg_df = spark.createDataFrame(historical_avg_with_index, ['day_index', 'day_of_week', 'average_tons'])
silo_actuals_df = spark.createDataFrame(silo_actuals, ['date', 'silo_wt_in_tons'])

# Create a calendar object with Thursday as the first weekday
calender_object = calendar.Calendar(firstweekday=3)  # Reference: https://www.geeksforgeeks.org/python-calendar-module-itermonthdays-method/

# Generate list of days for June 2023
month_days_generated = list(calender_object.itermonthdays4(2023, 6))

# Filter days for June
filter_june_month = lambda x: x[1] >= 6
june_days = list(filter(filter_june_month, month_days_generated))

# Function to calculate ISO week number
def weeknum(dt):
    return dt.isocalendar()[1]

# Add custom week number to each day in June
def add_custom_week_number(element):
    year, month, day, week_day = element
    # Add a timedelta of 4 days to calculate the offset date
    offset_date = datetime.datetime(year, month, day) + datetime.timedelta(days=4)  # Reference: https://stackoverflow.com/questions/60816403/get-week-number-with-week-start-day-different-than-monday-python
    return (f'{month}/{day}/{year}', month, day, week_day, weeknum(offset_date))

# Map custom week number to each day in June
intermediate_result = list(map(add_custom_week_number, june_days))

intermediate_result_df = spark.createDataFrame(intermediate_result, ['date', 'month', 'day', 'day_index', 'custom_week_num'])

result = (
    intermediate_result_df.
    join(historical_avg_df, ['day_index'], 'left').
    join(silo_actuals_df, ['date'], 'left').
    selectExpr(
        "date",
        "month",
        "day",
        "day_index",
        "COALESCE(silo_wt_in_tons, average_tons) AS silo_wt_in_tons",
        "custom_week_num"
    ).
    withColumn("weekly_total_tons", F.when(
        F.col('day_index') == 2, 
        F.sum("silo_wt_in_tons").over(Window.partitionBy(F.col("custom_week_num"))).
        otherwise(F.lit(''))
    ). 
    withColumn("mtd_running_total_tons", F.sum("silo_wt_in_tons").over(Window.orderBy('month','day').rowsBetween(Window.unboundedPreceding, 0))).  # Reference: https://www.statology.org/pyspark-cumulative-sum/
    withColumn("monthly_grand_total", F.when(
        F.col('day') == 30, 
        F.sum("silo_wt_in_tons").over(Window.partitionBy(F.col("month")))).
        otherwise(F.lit(''))
    )
).drop("month","day","day_index","custom_week_num")

# Display the result DataFrame
result.show(50, False)
