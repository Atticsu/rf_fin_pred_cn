#%%
import os
import time
import random
import pandas as pd
import numpy as np
from tqdm import tqdm
import tushare as ts
# %%
def random_sleep(mean=3 ,std =1):
    rand_sleep = abs(np.random.normal(mean, std)) #随机sleep以反爬
    time.sleep(rand_sleep)

def save_progress(date):
    with open("AshareData/progress.txt", "w") as f:
        f.write(date)

def load_progress():
    if os.path.exists("AshareData/progress.txt"):
        with open("AshareData/progress.txt", "r") as f:
            return f.read().strip()
    return None

# %% Main script

date_range = pd.date_range(start='2010-01-01', end='2024-10-06')
date_list = date_range.to_list()
date_list_str = [date.strftime('%Y%m%d') for date in date_list]

# Load progress if it exists
last_saved_date = load_progress()

# If progress exists, resume from the next date
if last_saved_date:
    start_index = date_list_str.index(last_saved_date) + 1
else:
    start_index = 0

# Initialize an empty dataframe to combine data
combined_df = pd.DataFrame()

# Loop over the dates, starting from the saved progress
for date in tqdm(date_list_str[start_index:]):
    pro = ts.pro_api()
    try:
        df = pro.daily(trade_date=date)
        df.to_feather(f"AshareData/{date}.feather")
        combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Save progress after each date
        save_progress(date)

        # Random sleep to avoid anti-scraping detection
        random_sleep(mean=1.2,std=0.5)

    except Exception as e:
        print(f"Error on {date}: {e}")
        break  # Optional: stop the loop in case of an error

# %%
