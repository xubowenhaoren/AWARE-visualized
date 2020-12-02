import pandas as pd


# This cleans up the dataframe from duplicate rows.
def clean_up_dataframe(df):
    # pprint(df)
    print("Total number of rows", len(df))
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.timestamp = df.timestamp.dt.tz_localize('UTC').dt.tz_convert('america/los_angeles')
    df.sort_values(by=['timestamp'], inplace=True)
    print("dropping duplicates")
    df.drop_duplicates(subset=None, keep="first", inplace=True)
    df['date'] = df.apply(lambda row: to_date(row), axis=1)
    print("New number of rows", len(df))
    return df


def to_date(row):
    return row['timestamp'].date()
