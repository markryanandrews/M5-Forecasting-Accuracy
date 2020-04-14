import datetime as dt
import pandas as pd

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
def build_sales(item_name, sales_df, calendar_df):
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