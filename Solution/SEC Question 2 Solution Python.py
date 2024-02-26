import pandas as pd
import numpy as np
import datetime as dt

month = input('Enter the month for the report ') # Input month for which we need the report
year = input('Enter year for the report ')       # Input year for which we need the report
# month = '06'
# year = '2023'
def generate_each_day_for_month(month,year):
    """"
    This function takes the month and year as input
    This function will return the dataframe with start to end date with name of day for that month of the year
    """
    monthyear = month + '-' + year
    df = pd.DataFrame({
        'date': pd.date_range(
            start=pd.Timestamp(monthyear),
            end=pd.Timestamp(monthyear) + pd.offsets.MonthEnd(0),
            freq='D'
        )
    })
    df['weekday'] = df['date'].dt.day_name()
    return(df)


# Data Import
actual_readings_in_june = pd.read_csv("silo_actuals.csv", sep=",", keep_default_na=False)
each_weekday_average = pd.read_csv("historical_averages.csv", sep=",", keep_default_na=False)

# converting the datatype to datetime for the join
actual_readings_in_june['date'] = pd.to_datetime(actual_readings_in_june['date'])

# generating the list of days in the month
MonthlyDaylist = generate_each_day_for_month(month,year)

# joining with the average by day of week
MonthlyDaylist = MonthlyDaylist.merge(each_weekday_average, left_on=['weekday'], right_on=['day'])
MonthlyDaylist = MonthlyDaylist[["date","weekday","average_tons"]].reset_index()

# joining with the actual data recorded for each month
MonthlyDaylist = MonthlyDaylist.merge(actual_readings_in_june, left_on=['date'], right_on=['date'], how='left')

# using actual reading for the days data is available else use avergae for the week day
MonthlyDaylist['daily_silo_wt_in_tons'] = np.where(MonthlyDaylist["silo_wt_in_tons"].notna(),MonthlyDaylist["silo_wt_in_tons"],MonthlyDaylist["average_tons"])
MonthlyDaylist = MonthlyDaylist[["date","weekday","daily_silo_wt_in_tons"]].reset_index()

# calculating the month to date running total
MonthlyDaylist['mtd_running_total_tons']=MonthlyDaylist['daily_silo_wt_in_tons'].rolling(len(MonthlyDaylist), min_periods=1).sum()

# calculating weekly total for each wednesday
MonthlyDaylist['week_running_total_tons']=MonthlyDaylist['daily_silo_wt_in_tons'].rolling(7, min_periods=1).sum()
MonthlyDaylist['weekly_total_tons']= np.where(MonthlyDaylist["weekday"] == 'Wednesday',MonthlyDaylist["week_running_total_tons"],np.nan )

# calculating monthly grand total
MonthlyDaylist['monthly_grand_total']=MonthlyDaylist['daily_silo_wt_in_tons'].rolling(len(MonthlyDaylist), min_periods=len(MonthlyDaylist)).sum()
MonthlyDaylist = MonthlyDaylist[["date","daily_silo_wt_in_tons","weekly_total_tons","mtd_running_total_tons","monthly_grand_total"]].reset_index()

print(MonthlyDaylist)
# exporting output
MonthlyDaylist.to_csv('Monthly_report.csv', index=False)

