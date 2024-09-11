import pandas as pd

def run(spark, filepath:str)->pd.DataFrame:
  """
  Args:
      Takes in Spark Session
      path: File Path to data storage
  Returns: pd.DataFrame
  """
  
  with open(filepath) as file:
    
    query = file.read()
    
    return spark.sql(query).toPandas()

def to_timstamp(df:pd.DataFrame, col_name:str)->pd.DataFrame:
  """
  Args:
      df (_type_): Pandas DataFrame
      col_name (_type_): Str

  Returns:
      Pandas DataFrame
  """
  
  df[col_name] = pd.to_datetime(df[col_name], unit='ms')
  return df

def rename_cols(df:pd.DataFrame, column_list:list)->pd.DataFrame:
  """
  Args:
      df (pd.DataFrame): Pandas DataFrame
      column_list (list): List

  Returns:
      pd.DataFrame: 
  """
  
  df.columns = [col for col in column_list]
  return df

def replace_nulls_with_str(df:pd.DataFrame, col_name:str)->pd.DataFrame:
  """
  Args:
      df (pd.DataFrame): _description_
      col_name (str): _description_

  Returns:
      pd.DataFrame: _description_
  """
  
  df[col_name].fillna('NULL', inplace=True)
  return df

def _trim(df:pd.DataFrame, col_name:str)->pd.DataFrame:
  """
  Args:
      df (pd.DataFrame): Pandas Dataframe
      col_name (str): Str

  Returns:
      pd.DataFrame
  """
  df[col_name] = df[col_name].apply(lambda x: x.strip())
  return df

def add_gov_provider(df:pd.DataFrame)->pd.DataFrame:
  """
  Args:
      df (pd.DataFrame): _description_

  Returns:
      pd.DataFrame: _description_
  """
  
  df["mpd_grped"] = df["mpd_grped"].apply(lambda x: "AB" if x in ["A", "B"] else x)
  
  return df

def add_member_over_65(df:pd.DataFrame)->pd.DataFrame:
  """
  Args:
      df (pd.DataFrame): _description_

  Returns:
      pd.DataFrame: _description_
  """
  df["data_start_date"] = pd.to_datetime(df["data_start_date"])
  df["birth_date"] = pd.to_datetime(df["birth_date"])
  df["day_diff"] = (df["data_start_date"] - df["birth_date"]).dt.days
  
  df["member_over_65"] = df["day_diff"].apply(lambda x: 1 if x >=65 else 0)
  
  df.drop("day_diff", axis=1, inplace=True)
  
  return df


def add_gov_provider(df:pd.DataFrame)->pd.DataFrame:
  """
  Args:
      df (pd.DataFrame): _description_

  Returns:
      pd.DataFrame: _description_
  """
  
  df["mpd_grped"] = df["mpd_grped"].apply(lambda x: "AB" if x in ["A", "B"] else x)
  
  return df

