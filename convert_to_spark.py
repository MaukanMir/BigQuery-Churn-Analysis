import pandas as pd

def run(spark, filepath:str)->pd.DataFrame:
  """_summary_

  Args:
      Takes in Spark Session
      path: File Path to data storage
  Returns: pd.DataFrame
  """
  
  with open(filepath) as file:
    
    query = file.read()
    
    return spark.sql(query).to_pandas()