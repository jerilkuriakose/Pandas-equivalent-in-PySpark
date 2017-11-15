
# coding: utf-8

# # CSV Files

# ## Pandas

# In[1]:


import pandas as pd

filepath = '../data/titanic.csv'

# Read the CSV file
df = pd.read_csv(filepath)


# **To view the top 5 rows**

# In[2]:


df.head()


# **Save dataframe to CSV**

# In[3]:


df.to_csv('file-pandasDF.csv')


# ## PySpark

# In[4]:


# import spark session
from pyspark.sql import SparkSession

# Create a session
spark = SparkSession.builder     .master('local')     .appName('csvFileHandling')     .config('spark.executor.memory', '1gb')     .config("spark.cores.max", "2")     .getOrCreate()

# initialise sparkContext
sc = spark.sparkContext


# In[5]:


filepath = '../data/titanic.csv'


# In[6]:


from pyspark.sql import SQLContext

# A SQLContext can be used create DataFrame, register DataFrame as tables,
# execute SQL over tables, cache tables, and read parquet files.
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv')     .options(header='true', inferschema='true')     .load(filepath) # this is your csv file


# In[7]:


df.show(5)


# **Save dataframe to CSV**

# In[8]:


# We use the 'overwrite' mode to avoid 'file already exists' error
df.write.csv('file-pysparkDF.csv', mode='overwrite')


# In[9]:


# To stop spark
spark.stop()


# # Text Files

# ## Pandas

# In[10]:


import pandas as pd

filepath = '../data/frogFox.txt'

# Read the CSV file
# sep (seperator) can be changed as per the requirement
# for eg., sep=' ', will make rows from words
df = pd.read_csv(filepath, sep='\n', header=None)


# In[11]:


df


# **Save as text file**

# In[12]:


# 'header=None' and 'index=none' will not save the 
# header and index data
df.to_csv('file-pandas.txt', header=None, sep='\n', index=None)


# ## PySpark

# In[13]:


# import spark session
from pyspark.sql import SparkSession

# Create a session
spark = SparkSession.builder     .master('local')     .appName('txtFileHandling')     .config('spark.executor.memory', '1gb')     .config("spark.cores.max", "2")     .getOrCreate()

# initialise sparkContext
sc = spark.sparkContext


# In[14]:


filepath = '../data/frogFox.txt'

# first we read the txt file
# then we split it using '\n'
# finally we convert it to dataframe
df = sc.textFile(filepath)     .map(lambda x: x.split('\n'))     .toDF()


# In[15]:


df.show()


# In[16]:


df.write.csv('file-pysparkDF.txt', mode='overwrite')


# In[17]:


spark.stop()


# # Parquet file

# ## Pandas

# In[18]:


import pandas as pd

filepath = '../data/titanic.csv'

# Read the CSV file
df = pd.read_csv(filepath)


# In[19]:


df.head()


# In[20]:


# save dataframe to parquet
df.to_parquet('file-pandas.parquet')


# In[21]:


# read parquet file
df1 = pd.read_parquet('file-pandas.parquet')
# need to use "engine='pyarrow'" if reading a 
# parquet file saved using PySpark
# need to install pyarrow "pip install pyarrow"


# In[22]:


df1.head()


# ## PySpark

# In[23]:


# import spark session
from pyspark.sql import SparkSession

# Create a session
spark = SparkSession.builder     .master('local')     .appName('txtFileHandling')     .config('spark.executor.memory', '1gb')     .config("spark.cores.max", "2")     .getOrCreate()

# initialise sparkContext
sc = spark.sparkContext


# In[24]:


filepath = '../data/titanic.csv'


# In[25]:


from pyspark.sql import SQLContext

# A SQLContext can be used create DataFrame, register DataFrame as tables,
# execute SQL over tables, cache tables, and read parquet files.
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv')     .options(header='true', inferschema='true')     .load(filepath) # this is your csv file


# In[26]:


df.show(5)


# In[27]:


# save dataframe as parquet
df.write.parquet('file-pyspark.parquet', mode='overwrite')


# In[28]:


# read parquet file
df1 = sqlContext.read.parquet('file-pyspark.parquet')


# In[29]:


df1.show(5)

