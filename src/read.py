import pandas as pd

def read_csv(filepath):
    df = pd.read_csv(filepath)
    print(df.head())
    return df

def read_json(filepath):
    df = pd.read_json(filepath)
    print(df.head())
    return df


def read(filepath):
    extension = filepath.lower().split('.')[-1]
    if (extension == "csv"):
        return read_csv(filepath)
    elif (extension == "json"):
        return read_json(filepath)