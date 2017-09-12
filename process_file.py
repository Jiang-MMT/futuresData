import csv
import os
import pandas as pd
from glob import glob
from pandas.tseries.offsets import *
from bdateutil import isbday
from pandas.tseries.holiday import USFederalHolidayCalendar


bday_us = CDay(calendar=USFederalHolidayCalendar())
bmth_us =CBMonthBegin(calendar=USFederalHolidayCalendar())
emth_us = CBMonthEnd(calendar=USFederalHolidayCalendar())


def filter_folder(directory):
    folder_list = []
    for folder in os.listdir(directory):
        folder_path = os.path.join(directory, folder)
        if not folder.startswith('.') and os.path.isdir(folder_path):
            folder_list.append(folder)
    return folder_list


def merge_files(directory):
    os.chdir(directory)
    folder_list = filter_folder(directory)
    for folder in folder_list:
        folder_path = os.path.join(directory, folder)
        if os.path.exists('{}.csv'.format(folder_path)):
            continue
        else:
            os.system('cat {}/*.csv >> temp.csv'.format(folder_path))
            #further process the combined file
            df = pd.read_csv('temp.csv', header=None)
            df.drop_duplicates(inplace=True)
            df.to_csv('{}.csv'.format(folder),header=False, index=False)
            os.system('rm temp.csv')


class getExpire():
    bday_us = CDay(calendar=USFederalHolidayCalendar())

    def __init__(self):
        pass

    @staticmethod
    def Cash_settle(date):
        pass

    @staticmethod
    def Third_last_business_day_of_the_maturing_delivery_month(date):
        return date + emth_us -bday_us*2

    @staticmethod
    def Eleven_business_days_prior_to_last_business_day_of_delivery_month(date):
        return date + emth_us -bday_us*11

    @staticmethod
    def Business_day_prior_to_USDA_Butter_Price_announcement(date):
        return date + emth_us - bday_us

    @staticmethod
    def Trading_terminates_at_the_close_of_business_on_the_third_business_day_prior_to_the_25th_calendar_day_of_the_month_preceding_the_delivery_month(date):
        return date - DateOffset(months=1) + Day(24) - bday_us*3

    @staticmethod
    def Seventeen_business_days_from_end_of_spot_month(date):
        return date + emth_us -bday_us*16

    @staticmethod
    def Business_day_prior_to_USDA_Milk_Price_announcement(date):
        return date + emth_us - bday_us

    @staticmethod
    def The_last_Thursday_of_the_contract_month(date):
        return date + LastWeekOfMonth(weekday=3)

    @staticmethod
    def First_Hong_Kong_Business_day_before_the_third_Wednesday_of_the_contract_month(date):
        return date + WeekOfMonth(week=2, weekday=2) - BDay(2)

    @staticmethod
    def The_tenth_business_day_of_the_contract_month(date):
        return date - DateOffset(months=1) + BMonthEnd() + bday_us*10

    @staticmethod
    def Third_to_last_business_day_of_the_contract_month(date):
        return date + emth_us - bday_us*2

    @staticmethod
    def Trading_terminates_at_the_close_of_business_on_the_last_business_day_of_the_month_preceding_the_delivery_month(date):
        return date - DateOffset(months=1) +emth_us

    @staticmethod
    def Eight_business_days_prior_to_last_business_day_of_delivery_month(date):
        return date + emth_us - bday_us*8

    @staticmethod
    def The_last_business_day_of_the_contract_month(date):
        return date + emth_us

    @staticmethod
    def Business_day_immediately_preceding_the_16th_calendar_day_of_the_contract_month(date):
        return date + SemiMonthBegin(day_of_month=16) - bday_us

    @staticmethod
    def The_business_day_prior_to_the_15th_calendar_day_of_the_contract_month(date):
        return date + SemiMonthBegin() - bday_us

    @staticmethod
    def Trading_terminates_three_business_days_prior_to_the_first_calendar_day_of_the_delivery_month(date):
        return date - DateOffset(months=1) + MonthBegin() - bday_us*3

    @staticmethod
    def The_15th_last_business_day_of_the_expiry_month(date):
        return date + emth_us - bday_us*14

    @staticmethod
    def Close_of_business_on_the_25th_day_of_the_month_prior_to_the_contract_month(date):
        if not isbday(date -DateOffset(months=1) + SemiMonthBegin(day_of_month=25)):
            return date -DateOffset(months=1) + SemiMonthBegin(day_of_month=25) -bday_us
        else:
            return date -DateOffset(months=1) + SemiMonthBegin(day_of_month=25)

    @staticmethod
    def Third_business_day_prior_to_the_end_of_the_delivery_month(date):
        return date + emth_us - bday_us*3

    @staticmethod
    def Trading_terminates_the_last_UK_business_day_of_the_second_month_prior_to_the_delivery_month(date):
        return date - DateOffset(months=2) + BMonthEnd() -BDay(1)

    @staticmethod
    def Third_last_business_day_before_the_maturing_delivery_month(date):
        return date + emth_us -bday_us*2

    @staticmethod
    def The_15th_of_the_spot_month_postponed_for_holidays(date):
        if isbday(date + SemiMonthBegin()):
            return date + SemiMonthBegin()
        else:
            return date + SemiMonthBegin() + bday_us

    @staticmethod
    def The_5th_of_the_delivery_month_If_it_is_a_non_working_day_the_first_trading_day_prior_to_this_day(date):
        if isbday(date + SemiMonthBegin(day_of_month=5)):
            return date + SemiMonthBegin(day_of_month=5)
        else:
            return date + SemiMonthBegin(day_of_month=5) - bday_us


def parse_contract_month(symbol_str):
    with open('month_codes.csv', 'r') as f:
        reader = csv.reader(f)
        month_codes_dict = {row[0]:row[1] for row in reader}
    year = symbol_str[-2:]
    month = month_codes_dict[symbol_str[2]]
    return year+month


def get_exp_dict(expirations):
    with open(expirations, 'r') as f:
        reader = csv.reader(f)
        reader.next()
        exp_dict = {row[0]: row[4] for row in reader}
        return exp_dict


def process_csv(directory, expirations):
    os.chdir(directory)
    exp_dict = get_exp_dict(expirations)

    for csvFileName in glob('*.csv'):
        if len(csvFileName) == 6:
            symbol = csvFileName[:2]
            if os.path.exists('{}/{}_master.csv'.format(directory, symbol)):
                continue
            else:
                if symbol not in exp_dict.keys():
                    continue
                elif symbol in exp_dict.keys():
                    function_name = exp_dict[symbol].replace(' ', '_')
                    df = pd.read_csv(csvFileName)
                    df['tradingDay'] = pd.to_datetime(df['tradingDay'])
                    df['contract_month'] = df.symbol.apply(parse_contract_month)
                    df['contract_month'] = pd.to_datetime(df['contract_month'], format='%y%B')
                    df.loc[(df['contract_month'].dt.year > 2040), 'contract_month'] = df.loc[(df['contract_month'].dt.year > 2040)].contract_month - DateOffset(years=100)
                    if function_name == 'Cash_settle':
                        df['expire_date'] = df['tradingDay']
                    else:
                        df['expire_date'] = df['contract_month'].apply(getattr(getExpire, function_name))
                    df['days_to_expire'] = (df['expire_date'] - df['tradingDay']).dt.days
                    df.loc[df['days_to_expire'] < 0] = None
                    df['tradingYear'] = df['tradingDay'].dt.year
                    df['tradingMonth'] = df['tradingDay'].dt.month
                    df['tradingDay_Day'] = df['tradingDay'].dt.day
                    df['contract_month'] = df['contract_month'].dt.strftime('%Y%b')
                    df['expire_Year'] = df['expire_date'].dt.year
                    df['expire_Month'] = df['expire_date'].dt.month
                    df['expire_Day'] = df['expire_date'].dt.day
                    df_ = df.loc[:, ['symbol', 'contract_month', 'tradingYear', 'tradingMonth', 'tradingDay_Day', 'expire_Year', 'expire_Month', 'expire_Day','days_to_expire', 'open', 'high', 'low', 'close', 'volume', 'openInterest']]
                    df_.set_index(['symbol', 'contract_month', 'expire_Year', 'expire_Month', 'expire_Day'], inplace=True)
                    df_.sort_index(level=['expire_Year', 'expire_Month', 'expire_Day'], inplace=True)
                    df_.to_csv('{}_master.csv'.format(symbol), index=True)
                    print 'processing of {} is completed!'.format(csvFileName)

if __name__ == '__main__':
    directory = os.getcwd()
    filter_folder(directory)
    merge_files(directory)
    print 'finish merging'
    expirations = 'downloaded_futures_expiration.csv'
    process_csv(directory, expirations)
