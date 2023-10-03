#!/usr/bin/env python
# coding: utf-8

# In[1]:


spark


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ETL_project').master("local").getOrCreate()
spark


# **Creating an input schema using StructType**

# In[3]:


# Importing required classes and Types 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, LongType


# In[4]:


# Constructing the schema

Schema = StructType([StructField('year', IntegerType(), nullable = True),
                        StructField('month', StringType(), True),
                        StructField('day', IntegerType(), True),
                        StructField('weekday', StringType(), True),
                        StructField('hour', IntegerType(), True),
                        StructField('atm_status', StringType(), True),
                        StructField('atm_id', StringType(), True),
                        StructField('atm_manufacturer', StringType(), True),
                        StructField('atm_location', StringType(), True),
                        StructField('atm_streetname', StringType(), True),
                        StructField('atm_street_number', IntegerType(), True),
                        StructField('atm_zipcode', IntegerType(), True),
                        StructField('atm_lat', DoubleType(), True),
                        StructField('atm_lon', DoubleType(), True),
                        StructField('currency', StringType(), True),
                        StructField('card_type', StringType(), True),
                        StructField('transaction_amount', IntegerType(), True),
                        StructField('service', StringType(), True),
                        StructField('message_code', StringType(), True),
                        StructField('message_text', StringType(), True),
                        StructField('weather_lat', DoubleType(), True),
                        StructField('weather_lon', DoubleType(), True),
                        StructField('weather_city_id', IntegerType(), True),
                        StructField('weather_city_name', StringType(), True),
                        StructField('temp', DoubleType(), True),
                        StructField('pressure', IntegerType(), True),
                        StructField('humidity', IntegerType(), True),
                        StructField('wind_speed', IntegerType(), True),
                        StructField('wind_deg', IntegerType(), True),
                        StructField('rain_3h', DoubleType(), True),
                        StructField('clouds_all', IntegerType(), True),
                        StructField('weather_id', IntegerType(), True),
                        StructField('weather_main', StringType(), True),
                        StructField('weather_description', StringType(), True)])


# In[6]:


df = spark.read.csv("/user/root/spar_nord_bank_atm/part-m-00000", header = False, schema = Schema)


# **Verifying the count of loaded data**

# In[7]:


df.count()


# **Taking a glance at the Schema,Dataframe and columns**

# In[8]:


df.printSchema()


# In[9]:


df.show(1)


# In[10]:


df.columns


# ## Creating the Dimension and Fact tables

# **Creating a data frame for the Location Dimension according to Target schema**

# In[22]:


# creating a temporary data frame and selecting required columns & making sure records are distinct
location = df.select('atm_location', 'atm_streetname', 'atm_street_number', 'atm_zipcode', 'atm_lat', 'atm_lon').distinct()


# In[23]:


from pyspark.sql.window import Window
from pyspark.sql.functions import *

# creating the primary key column
dim_location = location.select(row_number().over(Window.orderBy(location[0])).alias("location_id"), "*")
dim_location.show(5)


# In[24]:


# renaming the columns as per requirement
DIM_LOCATION = dim_location.withColumnRenamed('atm_location','location')                            .withColumnRenamed('atm_streetname','streetname')                            .withColumnRenamed('atm_street_number','street_number')                            .withColumnRenamed('atm_zipcode','zipcode')                            .withColumnRenamed('atm_lat','lat')                            .withColumnRenamed('atm_lon','lon')


# In[25]:


# checking that all required columns are present and named correctly
DIM_LOCATION.columns


# In[26]:


# validating the count of the dataframe
DIM_LOCATION.count()


# **DIM_ATM**

# In[16]:


# creating a temporary df and selecting required columns
atm = df.select('atm_id', 'atm_manufacturer', 'atm_lat', 'atm_lon')


# In[17]:


# renaming the column atm_id to atm_number as per requirement
atm = atm.withColumnRenamed('atm_id', 'atm_number')


# In[27]:


# joining the dim_location and atm dataframes
atm = atm.join(dim_location, on = ['atm_lat', 'atm_lon'], how = "left")


# In[28]:


# checking columns in the joined df
atm.columns


# In[29]:


# selecting the required columns and making sure records are distinct
dim_atm = atm.select('atm_number', 'atm_manufacturer', 'location_id').distinct()


# In[30]:


DIM_ATM = dim_atm.select(row_number().over(Window.orderBy(dim_atm[0])).alias('atm_id'), 'atm_number', 'atm_manufacturer', col('location_id').alias('atm_location_id'))


# In[31]:


# checking that all required columns are present and named correctly
DIM_ATM.columns


# In[32]:


# validating the count of the dataframe
DIM_ATM.count()


# In[33]:


DIM_ATM.show(5)


# **DIM_DATE**

# In[34]:


# creating a temporary df and selecting required columns
date = df.select('year', 'month', 'day', 'hour', 'weekday')


# In[37]:


date = date.withColumn('full_date', concat_ws('-', date.year, date.month, date.day))


# In[38]:


date = date.withColumn('full_time', concat_ws(':', date.hour, lit('00'), lit('00')))


# In[39]:


date = date.withColumn('full_date_time', concat_ws(' ', date.full_date, date.full_time))


# In[40]:


pattern = 'yyyy-MMM-dd HH:mm:ss'
date = date.withColumn('full_date_time', unix_timestamp(date.full_date_time, pattern).cast('timestamp'))


# In[41]:


date.show(5, truncate = False)


# In[42]:


# selecting the required columns and making sure records are distinct
date = date.select('full_date_time', 'year', 'month', 'day', 'hour', 'weekday').distinct()


# In[43]:


DIM_DATE = date.select(row_number().over(Window.orderBy(date[0])).alias('date_id'), '*')


# In[44]:


# checking that all required columns are present and named correctly
DIM_DATE.columns


# In[45]:


# validating the count of the dataframe
DIM_DATE.count()


# **DIM_CARD_TYPE**

# In[46]:


# creating a temporary df and selecting required columns and making sure records are distinct
card_type =  df.select('card_type').distinct()


# In[47]:


# creating the primary key column
DIM_CARD_TYPE = card_type.select(row_number().over(Window.orderBy(card_type[0])).alias("card_type_id"), "*")
DIM_CARD_TYPE.show(5)


# In[48]:


# checking that all required columns are present and named correctly
DIM_CARD_TYPE.columns


# In[49]:


# validating the count of the dataframe
DIM_CARD_TYPE.count()


# **Creating the Transaction Fact Table according to Target Model**

# In[50]:


fact_loc = df.withColumnRenamed('atm_location','location')    .withColumnRenamed('atm_streetname','streetname')    .withColumnRenamed('atm_street_number','street_number')    .withColumnRenamed('atm_zipcode','zipcode')    .withColumnRenamed('atm_lat','lat')    .withColumnRenamed('atm_lon','lon')

# joining original dataframe with DIM_LOCATION
fact_loc = fact_loc.join(DIM_LOCATION, on = ['location', 'streetname', 'street_number', 'zipcode', 'lat', 'lon'], how = "left")


# In[51]:


fact_loc = fact_loc.withColumnRenamed('atm_id', 'atm_number').withColumnRenamed('location_id', 'atm_location_id')

# joining the dataframe with DIM_ATM
fact_atm = fact_loc.join(DIM_ATM, on = ['atm_number', 'atm_manufacturer', 'atm_location_id'], how = "left")


# In[52]:


# performing necessary transformations, same as done to atm table
fact_atm = fact_atm.withColumnRenamed('atm_location_id', 'weather_loc_id')


# In[53]:


# joining the dataframe with DIM_DATE
fact_date = fact_atm.join(DIM_DATE, on = ['year', 'month', 'day', 'hour', 'weekday'], how = "left")


# In[54]:


# joining the dataframe with DIM_CARD_TYPE
fact_atm_trans = fact_date.join(DIM_CARD_TYPE, on = ['card_type'], how = "left")


# In[55]:


# creating primary key of fact table
FACT_ATM_TRANS = fact_atm_trans.withColumn("trans_id", row_number().over(Window.orderBy('date_id')))


# In[56]:


# viewing the list of columns
FACT_ATM_TRANS.columns


# In[57]:


# selecting and arranging only the required columns according to the target model
FACT_ATM_TRANS = FACT_ATM_TRANS.select('trans_id', 'atm_id', 'weather_loc_id', 'date_id', 'card_type_id', 
'atm_status', 'currency', 'service', 'transaction_amount', 'message_code', 'message_text', 'rain_3h', 
'clouds_all', 'weather_id', 'weather_main', 'weather_description')


# In[58]:


# checking that all required columns are present and named correctly
FACT_ATM_TRANS.columns


# In[59]:


# validating the count of the dataframe
FACT_ATM_TRANS.count()


# In[60]:


FACT_ATM_TRANS.show(1)


# ### Writing the PySpark Dataframes to tables to S3 bucket

# In[63]:


def pushtos3(table_name,file_name):
    table_name.write.format('csv').option('header','false').save(f's3://aws-logs-555313307931-us-east-1/spar_nord_bank/{file_name}', mode='overwrite')


# In[64]:


pushtos3(DIM_LOCATION,'dim_location')
pushtos3(DIM_ATM,'dim_atm')
pushtos3(DIM_DATE,'dim_date')
pushtos3(DIM_CARD_TYPE,'dim_card_type')
pushtos3(FACT_ATM_TRANS,'fact_atm_trans')


# In[ ]:




