
# coding: utf-8

# ## Pandas

# In[1]:


import pandas as pd


# In[2]:


filepath = '../data/titanic.csv'


# **Read the CSV file**

# In[3]:


df = pd.read_csv(filepath)


# **To view the top 5 rows**

# In[4]:


df.head()


# **Save dataframe to CSV**

# In[5]:


df.to_csv('pandasDF.csv')


# ## PySpark

# In[6]:


# import spark session
from pyspark.sql import SparkSession

# Create a session
spark = SparkSession.builder     .master('local')     .appName('csvFileHandling')     .config('spark.executor.memory', '1gb')     .config("spark.cores.max", "2")     .getOrCreate()

# initialise sparkContext
sc = spark.sparkContext


# In[7]:


filepath = '../data/titanic.csv'


# In[8]:


from pyspark.sql import SQLContext

# A SQLContext can be used create DataFrame, register DataFrame as tables,
# execute SQL over tables, cache tables, and read parquet files.
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv')     .options(header='true', inferschema='true')     .load(filepath) # this is your csv file


# In[9]:


df.show(5)


# **Save dataframe to CSV**

# In[10]:


# We use the 'overwrite' mode to avoid 'file already exists' error
df.write.csv('pysparkDF.csv', mode='overwrite')


# In[11]:


# To stop spark
spark.stop()

