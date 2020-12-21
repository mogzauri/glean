#!/usr/bin/env python
# coding: utf-8

# In[339]:


# Technical Interview Glean
# Jeremy Johnson
# December 21, 2020


# In[340]:


import sys
get_ipython().system('{sys.executable} -m pip install pyspark')


# In[431]:


#Import Packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import min as min_, max as max_
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, DoubleType, LongType
import pyspark.sql.functions as funcs
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
import pandas as pd
import numpy as np


# In[366]:


#Initiate Spark Session

spark = SparkSession.builder     .master("local")     .appName("Glean Generator")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# In[367]:


# Create Schemas
invoice_schema = StructType([
    StructField("invoice_id", StringType()),
    StructField("invoice_date", DateType()),
    StructField("due_date", DateType()),
    StructField("period_start_date", DateType()),
    StructField("period_end_date", DateType()),
    StructField("total_amount", DoubleType()),
    StructField("canonical_vendor_id", StringType()), 
])

line_item_schema = StructType([
    StructField("invoice_id", StringType()),
    StructField("line_item_id", StringType()),
    StructField("period_start_date", DateType()),
    StructField("period_end_date", DateType()),
    StructField("total_amount", DoubleType()),
    StructField("canonical_line_item_id", StringType()), 
])

glean_schema = StructType([
    StructField('glean_id', StringType()),
    StructField('glean_date', DateType()),
    StructField('glean_text', StringType()),
    StructField('glean_type', StringType()),
    StructField('glean_location', StringType()),
    StructField('invoice_id', StringType()),
    StructField('canonical_vendor_id', StringType())
])


# In[368]:


#Read in CSVs
invoice_df = spark.read.csv('invoice.csv', header=True, schema=invoice_schema)
line_item_df = spark.read.csv('line_item.csv', header=True, schema=line_item_schema)


# In[369]:


#create an array of unique invoice ids and unique vendor ids
invoice_ids = invoice_df.toPandas()['invoice_id'].unique()
vendor_ids = invoice_df.toPandas()['canonical_vendor_id'].unique()


# In[370]:


# Create glean type 1

#Create a window by vendor and calculate difference in invoice dates
window = Window.partitionBy('canonical_vendor_id').orderBy('invoice_date')

invoice_df = invoice_df.withColumn("days_passed", funcs.datediff(invoice_df.invoice_date, 
                                  funcs.lag(invoice_df.invoice_date, 1).over(window)))


# In[371]:


glean1 = invoice_df[invoice_df['days_passed'] > 90]

glean1 = glean1.withColumn("months_passed", funcs.round(funcs.col("days_passed")/30))

glean1 = glean1.withColumn("glean_type", funcs.lit('vendor_not_seen_in_a_while'))

glean1 = glean1.withColumn("glean_text", 
                           funcs.concat(funcs.lit('First new bill in '), funcs.col('months_passed'), funcs.lit(' months from vendor '), funcs.col('canonical_vendor_id')))

glean1 = glean1.withColumn('glean_location', funcs.lit('invoice'))


# In[372]:


##glean type 2 accrual_alert
#create Alias to join DFs
invoice = invoice_df.alias('invoice')
line_item = line_item_df.alias('line_item')
#left join the invoice data with the line item data, joining on invoice_id
joined_table = invoice.join(line_item, invoice.invoice_id == line_item.invoice_id, how = 'left')


# In[373]:


glean2 = joined_table.groupBy(invoice.invoice_id, invoice.canonical_vendor_id, invoice.invoice_date, invoice.period_end_date).agg(max_('line_item.period_end_date').alias('max_line_end_date'))


# In[374]:


glean2 = glean2.withColumn('end_date', funcs.greatest('max_line_end_date',invoice.period_end_date))

glean2 = glean2.withColumn('difference', funcs.datediff('end_date', 'invoice_date'))

glean2 = glean2[glean2['difference'] > 90]

glean2 = glean2.withColumn('glean_location', funcs.lit('invoice'))

glean2 = glean2.withColumn("glean_type", funcs.lit('accrual_alert'))

glean2 = glean2.withColumn("glean_text", funcs.concat(funcs.lit('Line items from vendor '),
                                                                funcs.col('canonical_vendor_id'),funcs.lit(' in this invoice cover future periods (through '), funcs.col('end_date'), funcs.lit(' )')))


# In[383]:


## Create Glean 3
##large_month_increase_mtd
# Group by vendorID and Month and create rolling windows for averages
invoices = invoice_df.withColumn('invoice_year', funcs.year('invoice_date'))
invoices = invoices.withColumn('invoice_month', funcs.month('invoice_date'))
invoices = invoices.withColumn('invoice_quarter', funcs.quarter('invoice_date'))


# In[384]:


invoices=invoices.groupBy(['canonical_vendor_id', 'invoice_year', 'invoice_month','invoice_quarter']).agg({'total_amount':'sum', 'invoice_date':'max'}).sort('canonical_vendor_id', 'invoice_year','invoice_month')


# In[385]:





# In[386]:


#deal with seconds
days = lambda i: i * 86400
#Create a window by vendor and calculate rolling spend average
w = (Window.partitionBy('canonical_vendor_id').orderBy(funcs.col('max(invoice_date)').cast('long')).rangeBetween(-days(365), 0))

invoices = invoices.withColumn('rolling_average_12m', funcs.avg("sum(total_amount)").over(w))


# In[387]:


glean3=invoices[((invoices['sum(total_amount)'] >= invoices['rolling_average_12m'] * 6) &
                 (invoices['sum(total_amount)'] < 1000) & (invoices['sum(total_amount)'] >= 100)) |
                 ((invoices['sum(total_amount)'] >= invoices['rolling_average_12m'] * 3) &
                 (invoices['sum(total_amount)'] < 10000) & (invoices['sum(total_amount)'] >= 1000)) |
                 ((invoices['sum(total_amount)'] >= invoices['rolling_average_12m'] * 1.5) &
                 (invoices['sum(total_amount)'] >= 10000)) 
                 ]
                 


# In[388]:


glean3 = glean3.withColumn('dollar_dif', funcs.col('sum(total_amount)') - funcs.col('rolling_average_12m'))
glean3 = glean3.withColumn('percent_dif', funcs.round(100 * funcs.col('dollar_dif') / funcs.col('rolling_average_12m')))


# In[389]:


glean3 = glean3.withColumn('glean_location', funcs.lit('vendor'))

glean3 = glean3.withColumn("glean_type", funcs.lit('large_month_increase_mtd'))

glean3 = glean3.withColumn("glean_text", funcs.concat(funcs.lit('Monthly spend with '), funcs.col('canonical_vendor_id'), 
                                                     funcs.lit(' is $'), funcs.col('dollar_dif'), funcs.lit(' ('),
                                                     funcs.col('percent_dif'), funcs.lit('%) higher than average')))

glean3 = glean3.withColumn("invoice_id", funcs.lit('n/a'))


# In[ ]:





# In[391]:


# Glean 4 - no_invoice_received


# In[452]:


glean4 = invoices.groupby('canonical_vendor_id').agg(funcs.collect_list("invoice_month").alias('month_list'), funcs.collect_list('invoice_quarter').alias('quarter_list'), funcs.collect_list('max(invoice_date)').alias('date_list'))


# In[453]:


glean4.show()


# In[ ]:





# In[454]:


glean4[funcs.size(glean4['date_list']) >= 3].show()


# In[462]:


def date_missing(list_z):
    it_list=list(range(0, len(list_z)))
    number_correct = 0
    next_month = 0
    triggers = []
    signal = 0
    for x in it_list:
        if funcs.month(list_z[x]) == next_month:
            number_correct = number_correct + 1
        else:
            number_correct = 0    
        
        if signal == 1 & number_correct == 0:
            triggers.append(list_z[x-1])
        else:
            none
        
        if number_correct >= 3:
            signal = 1
        else: 
            singal = 0
        
        if x == 12:
            next_month = 1
        else:
            next_month = x + 1
    return triggers
    
        


# In[463]:


#create user defined function
dm_udf = udf(date_missing, LongType())


# In[466]:


glean4 = glean4.withColumn('trigger_date', dm_udf('date_list'))
glean4 = glean4[funcs.size(glean4['trigger_date']) > 0]
glean4 = glean4.withColumn('common_day', funcs.median(funcs.day('date_list')))
glean4 = glean4.withColumn("glean_type", funcs.lit('no_invoice_received'))
glean4 = glean4.withColumn("location", funcs.lit('vendor'))
glean4 = glean4.withColumn("glean_text", funcs.concat(funcs.col('canonical_vendor_id'), funcs.lit('  generally charges between on '),
                                                     funcs.col('common_day'), funcs.lit(' day of each month invoices are sent. On '), funcs.col('trigger_date'), 
                                                      funcs.lit('an invoice has not been recieve from ', funcs.col('canonical_vendor_id'))))


# In[476]:


# Assemble Glean CSV


# In[482]:


df1=glean1['invoice_id', 'glean_text', 'glean_type', 'invoice_date', 'glean_location', 'canonical_vendor_id']
df2=glean2['invoice_id', 'glean_text', 'glean_type', 'invoice_date', 'glean_location', 'canonical_vendor_id']
df3=glean3['invoice_id', 'glean_text', 'glean_type', 'max(invoice_date)', 'glean_location', 'canonical_vendor_id']
df4=glean4['invoice_id', 'glean_text', 'glean_type', 'max(invoice_date)', 'glean_location', 'canonical_vendor_id']


# In[483]:


df1_2 = df1.union(df2)
df3_4 = df3.union(df4)
df = df1_2.union(df3_4)
df.toPandas().to_csv('gleans.csv')


# In[484]:





# In[ ]:




