import datetime as dt
import numpy as np
import statsmodels.graphics.tsaplots as smt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


### DATA CLEANING / MANIPULATING FUNCTIONS ###

def log_frame(f):
    def wrapper(dataf, *args, **kwargs):
        tic = dt.datetime.now()
        result = f(dataf, *args, **kwargs)
        toc = dt.datetime.now()
        print(f"{f.__name__} took {toc - tic}, shape = {result.shape}")
        return result
    return wrapper

@log_frame
def start_pipeline(dataf):
    return dataf.copy()

@log_frame
def clean_sell_price(dataf):
    dataf['store_id'] = dataf['store_id'].astype('category')
    return dataf

@log_frame
def clean_calendar(dataf): 
    #Date/Time Types
    dataf['date'] = pd.to_datetime(dataf['date'])
    dataf['weekday'] = dataf['date'].dt.day_name()
    dataf['wday'] = dataf['date'].dt.dayofweek
    dataf['month'] = dataf['date'].dt.month
    dataf['year'] = dataf['date'].dt.year
    #Event Types
    dataf['event_name_1'] = dataf['event_name_1'].astype('category')
    dataf['event_type_1'] = dataf['event_type_1'].astype('category')
    dataf['event_name_2'] = dataf['event_name_2'].astype('category')
    dataf['event_type_2'] = dataf['event_type_2'].astype('category')
    
    dataf.set_index('date' , inplace = True)
    return dataf

@log_frame
def clean_sales(dataf):
    dataf['dept_id'] = dataf['dept_id'].astype('category')
    return dataf

@log_frame
def filter_item(item_name, sales_df, calendar_df):
    """ 
    Function: Combines Walmart Item Sales. 
      
    Attributes: 
        item_name (string): Unique Item
        sales_df (pandas.DataFrame) 
        calendar_df (pandas.DataFrame)
    
    Returns:
        dataf (pandas.DataFrame): Complete Unique Item Sale Information
    """
    # get sales data from sales (d-columns)
    d_cols = [c for c in sales_df.columns if 'd_' in c] # sales data columns
    dataf = sales_df.loc[sales_df['id'] == item_name].set_index('id')[d_cols].T
    
    # merge sales with calendar data
    dataf =  pd.merge(left = dataf , right = calendar_df, left_index = True, right_on = 'd')
    return dataf.drop(columns = ['d'])

@log_frame
def item_sales(item_name, sales_df, calendar_df):
    item_sale_df = filter_item(item_name, sales_df, calendar_df)
    return item_sale_df.iloc[:,:1]
    

### TIME SERIES FUNCTIONS ### 
def tsplot(y , title, lags = None, diff = 0, figsize = (16,8)):
    '''
    Examine patterns with
    Time Series plot,
    Histogram plot,
    ACF and PACF plots
    '''
    
    if diff > 5:
        return "Error: diff too high, Please choose [0-5]"
    if diff > 0:
        y = y.diff(diff)[diff:]  
    
    fig, ((ts_ax, hist_ax), (acf_ax, pacf_ax)) = plt.subplots(2, 2, figsize=figsize)
    #fig.suptitle('Time Series Analysis', fontsize =16)

    y.plot(ax = ts_ax)
    ts_ax.set_title(title, fontsize = 14, fontweight = 'bold')
    y.plot(ax = hist_ax, kind = 'hist', bins = 25)
    hist_ax.set_title(title, fontsize = 14, fontweight = 'bold')
    smt.plot_acf(y, lags = lags, ax = acf_ax)
    smt.plot_pacf(y, lags = lags, ax = pacf_ax)
    [ax.set_xlim(-.5) for ax in [acf_ax, pacf_ax]]
    sns.despine()
    plt.tight_layout()
    return ts_ax, hist_ax, acf_ax, pacf_ax
    
    
    
    
    
    
    