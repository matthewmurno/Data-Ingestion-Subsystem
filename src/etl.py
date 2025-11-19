import pandas as pd

from transform_healthcare import transform_healthcare

def transform(df: pd.DataFrame) -> pd.DataFrame:
    return transform_healthcare(df)

#dataset api
#"hf://datasets/Kilos1/healthcare_dataset/healthcare_dataset.csv"



