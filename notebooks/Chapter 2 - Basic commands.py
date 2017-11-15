
# coding: utf-8

# ## Pandas

# In[1]:


import pandas as pd


# In[2]:


filepath = '../data/titanic.csv'


# In[3]:


df = pd.read_csv(filepath)


# In[4]:


# Prints the first 5 rows.
df.head()


# In[5]:


# Returns all column names as a numpy array.
df.columns


# In[6]:


df.dtypes


# In[7]:


df.shape[0] # but the fastest is len(df.index)


# In[8]:


# Selecting particular columns and showing the top 5
df[['Name', 'Age']].head()


# In[9]:


df.describe()


# In[10]:


df['Pclass'].unique()


# ## PySpark

# In[11]:


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


# In[12]:


filepath = '../data/titanic.csv'


# In[13]:


# Create a session
spark = SparkSession.builder     .master('local')     .appName('csvFileHandling')     .config('spark.executor.memory', '1gb')     .config("spark.cores.max", "2")     .getOrCreate()

# initialise sparkContext
sc = spark.sparkContext


# In[14]:


# A SQLContext can be used create DataFrame, register DataFrame as tables,
# execute SQL over tables, cache tables, and read parquet files.
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv')     .options(header='true', inferschema='true')     .load(filepath) # this is your csv file


# In[15]:


# Prints the first ``n`` rows.
df.show(5)


# In[16]:


# Returns all column names as a list.
df.columns


# In[17]:


# Prints out the schema in the tree format.
df.printSchema()


# In[18]:


# Returns the number of rows in dataframe
df.count()


# In[19]:


# Gets required columns and prints the first ``n`` rows
df[['Name', 'Age']].show(5)


# In[20]:


# Get the stats of the dataframe
# Such as count, mean, stddev, min, and max
df.describe().show()


# In[21]:


# To get unique values
df[['Pclass']].distinct().show(5)

