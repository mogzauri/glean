#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
These are the functions that generate gleans

@author: jeremiahjohnson
"""

from pyspark.sql import SparkSession
from schema.schema import invoice_schema, line_item_schema
from pyspark.sql.window import Window
import pyspark.sql.functions as funcs
from pyspark.sql.types import IntegerType, LongType, ArrayType, DateType
from pyspark.sql.functions import udf
from datetime import datetime, timedelta
from pyspark.sql.functions import max as max_
import pandas as pd
from dateutil.relativedelta import relativedelta

"""
Glean 1	
logic:
	1) don't trigger if invoice received from the vendor for the first time
	2) trigger if invoice received from the vendor and it's > 90 days since last `invoice_date`
"""

def glean_1(invoice_df):
    window = Window.partitionBy('canonical_vendor_id').orderBy('invoice_date')
    invoice_df = invoice_df.withColumn("days_passed", funcs.datediff(invoice_df.invoice_date, 
                                                      funcs.lag(invoice_df.invoice_date, 1).over(window)))
    glean1 = invoice_df[invoice_df['days_passed'] > 90]
    glean1 = glean1.withColumn("months_passed", funcs.round(funcs.col("days_passed")/30))
    glean1 = glean1.withColumn("glean_type", funcs.lit('vendor_not_seen_in_a_while'))
    glean1 = glean1.withColumn("glean_text", funcs.concat(funcs.lit('First new bill in '), 
                                                          funcs.col('months_passed'), 
                                                          funcs.lit(' months from vendor '), 
                                                          funcs.col('canonical_vendor_id')))
    glean1 = glean1.withColumn('glean_location', funcs.lit('invoice'))
    glean1 = glean1.withColumn('glean_date', funcs.col('invoice_date'))
    return glean1

"""
glean 2

1) trigger if `period_end_date` for invoice or any line item > 90 days after `invoice_date`
	   If there are multiple end dates, pick the last one.

"""


def glean_2(invoice_df, line_item_df):
    invoice = invoice_df.alias('invoice')
    line_item = line_item_df.alias('line_item')
    joined_table = invoice.join(line_item, invoice.invoice_id == line_item.invoice_id, how = 'left')
    glean2 = joined_table.groupBy(invoice.invoice_id, invoice.canonical_vendor_id, 
                                  invoice.invoice_date, invoice.period_end_date).agg(max_('line_item.period_end_date').alias('max_line_end_date'))
    glean2 = glean2.withColumn('end_date', funcs.greatest('max_line_end_date',invoice.period_end_date))
    glean2 = glean2.withColumn('difference', funcs.datediff('end_date', 'invoice_date'))
    glean2 = glean2[glean2['difference'] > 90]
    glean2 = glean2.withColumn('glean_location', funcs.lit('invoice'))
    glean2 = glean2.withColumn("glean_type", funcs.lit('accrual_alert'))
    glean2 = glean2.withColumn("glean_text", funcs.concat(funcs.lit('Line items from vendor '),
                                                                funcs.col('canonical_vendor_id'),funcs.lit(' in this invoice cover future periods (through '), funcs.col('end_date'), funcs.lit(' )')))
    glean2 = glean2.withColumn('glean_date', funcs.col('invoice_date'))
    return glean2

"""
glean 3

logic:
	1) trigger if monthly spend > $10K and it increased > 50% of average spend over last 12 months. 
    If monthly spend is less than $10K, > 200%. If less than $1K, > 500%. If less than $100, don't trigger the glean. 
    Spend is sum of invoice `total_amount`.
"""


def glean_3(invoice_df):
    invoices = invoice_df.withColumn('invoice_year', funcs.year('invoice_date'))
    invoices = invoices.withColumn('invoice_month', funcs.month('invoice_date'))
    invoices = invoices.withColumn('invoice_quarter', funcs.quarter('invoice_date'))
    invoices=invoices.groupBy(['canonical_vendor_id', 'invoice_year', 'invoice_month',
                               'invoice_quarter']).agg({'total_amount':'sum', 'invoice_date'
                                                        :'max'}).sort('canonical_vendor_id', 
                                                                      'invoice_year','invoice_month')
    days = lambda i: i * 86400

    w = (Window.partitionBy('canonical_vendor_id').orderBy(funcs.col('max(invoice_date)').cast('long')).rangeBetween(-days(365), 0))

    invoices = invoices.withColumn('rolling_average_12m', funcs.avg("sum(total_amount)").over(w))


    glean3=invoices[((invoices['sum(total_amount)'] >= invoices['rolling_average_12m'] * 6) &
                 (invoices['sum(total_amount)'] < 1000) & (invoices['sum(total_amount)'] >= 100)) |
                 ((invoices['sum(total_amount)'] >= invoices['rolling_average_12m'] * 3) &
                 (invoices['sum(total_amount)'] < 10000) & (invoices['sum(total_amount)'] >= 1000)) |
                 ((invoices['sum(total_amount)'] >= invoices['rolling_average_12m'] * 1.5) &
                 (invoices['sum(total_amount)'] >= 10000)) 
                 ]

    glean3 = glean3.withColumn('dollar_dif', funcs.col('sum(total_amount)') - funcs.col('rolling_average_12m'))
    glean3 = glean3.withColumn('percent_dif', funcs.round(100 * funcs.col('dollar_dif') / funcs.col('rolling_average_12m')))
    glean3 = glean3.withColumn('glean_location', funcs.lit('vendor'))
    glean3 = glean3.withColumn("glean_type", funcs.lit('large_month_increase_mtd'))
    glean3 = glean3.withColumn("glean_text", funcs.concat(funcs.lit('Monthly spend with '), funcs.col('canonical_vendor_id'), 
                                                     funcs.lit(' is $'), funcs.col('dollar_dif'), funcs.lit(' ('),
                                                     funcs.col('percent_dif'), funcs.lit('%) higher than average')))

    glean3 = glean3.withColumn("invoice_id", funcs.lit('n/a'))
    glean3 = glean3.withColumn('glean_date', funcs.col('max(invoice_date)'))
    return glean3
"""
glean4 missing invoice
"""


def glean_4(invoice_df):
    
    invoices = invoice_df.withColumn('invoice_year', funcs.year('invoice_date'))    
    invoices = invoices.withColumn('invoice_month', funcs.date_trunc('mon', 'invoice_date'))
    invoices = invoices.withColumn('invoice_quarter', funcs.date_trunc('quarter', 'invoice_date'))
    invoices=invoices.groupBy(['canonical_vendor_id', 'invoice_year', 'invoice_month','invoice_quarter']).agg({'total_amount':'sum', 'invoice_date':'max'}).sort('canonical_vendor_id', 'invoice_year','invoice_month')


    glean4 = invoices.groupby('canonical_vendor_id').agg(funcs.collect_list("invoice_month").alias('month_list'), funcs.collect_list('invoice_quarter').alias('quarter_list'), funcs.collect_list('max(invoice_date)').alias('date_list'))


    def date_missing_m(xy):
        it_list=list(range(0, len(xy)))
        number_correct = 0
        next_month = 0
        triggers = []
        signal = 0
        for x in it_list:
            if xy[x].month == next_month:
                number_correct = number_correct + 1
            else:
                number_correct = 0    

            if ((signal == 1) and (number_correct == 0)):
                triggers.append(xy[x-1])
            else:
                None

            if number_correct >= 3:
                signal = 1
            else: 
                signal = 0

            if xy[x].month == 12:
                next_month = 1
            else:
                next_month = xy[x].month + 1

        return triggers

    udf_myFunction = udf(date_missing_m, ArrayType(DateType(), True))

    glean4 = glean4.withColumn('glean4_m', udf_myFunction('month_list'))

    def date_missing_q(xy):
        it_list=list(range(0, len(xy)))
        number_correct = 0
        next_quarter = 0
        triggers = []
        signal = 0
        for x in it_list:
            if xy[x].month == next_quarter:
                number_correct = number_correct + 1
            else:
                number_correct = 0    

            if ((signal == 1) and (number_correct == 0)):
                triggers.append(xy[x-1])
            else:
                None

            if number_correct >= 2:
                signal = 1
            else: 
                signal = 0

            if xy[x].month == 10:
                next_quarter = 1
            elif xy[x].month == 1:
                next_quarter = 4
            elif xy[x].month == 4:
                next_quarter = 7
            elif xy[x].month == 7:
                next_quarter = 10


        return triggers

    udf_myFunction2 = udf(date_missing_q, ArrayType(DateType(), True))

    glean4 = glean4.withColumn('glean4_q', udf_myFunction2('quarter_list'))
    
    def common_day(xy):
        days = []
        if len(xy) > 0:
            for x in xy:
                days.append(x.day)
            day = max(set(days), key=days.count)
        else:
            day = []

        return day

    udf_myFunction3 = udf(common_day)
    
    glean4 = glean4.withColumn('common_day', udf_myFunction3('date_list'))

    def trigger_m(xy):
        dates = []
        if len(xy) < 1:
            None
        else:
            for x in xy:
                trig_date = x + relativedelta(months = 1)
                dates.append(trig_date)
        return dates


    udf_myFunction4 = udf(trigger_m, ArrayType(DateType(), True))
    
    glean4 = glean4.withColumn('m_trigger_dates', udf_myFunction4(glean4['glean4_m']))
    
    def trigger_q(xy):
        dates = []
        if len(xy) < 1:
            None
        else:
            for x in xy:
                trig_date = x + relativedelta(months = 3)
                dates.append(trig_date)
        return dates


    udf_myFunction5 = udf(trigger_q, ArrayType(DateType(), True))
    
    glean4 = glean4.withColumn('q_trigger_dates', udf_myFunction5(glean4['glean4_q']))
    glean4 = glean4.withColumn('m_trigger_dates', funcs.array('m_trigger_dates'))
    glean4 = glean4.withColumn('glean_dates', funcs.explode('m_trigger_dates'))
    pd_glean4 = glean4.toPandas()
    pd_glean4['list_len'] = pd_glean4['glean_dates'].apply(len)
    df=pd_glean4[pd_glean4['list_len'] > 0]
    df['common_day'] = df['common_day'].astype(int)
    
    def last_day_of_month(any_day):
        next_month = any_day + relativedelta(day=31)
        last_day =  next_month + timedelta(days=next_month.day) - relativedelta(day=1)
        return(last_day)
    
    def date_ranger(any_day):
        last_day = last_day_of_month(any_day)
        dates=pd.date_range((any_day), last_day)
        return dates
    
    df = df.explode('glean_dates')
    df['date_range'] = df['glean_dates'].apply(date_ranger)
    df = df.explode('date_range')
    df['gleans'] =  df['canonical_vendor_id'].astype(str) + ' generally charges on ' + df['common_day'].astype(str) + ' of the each month invoices are sent. On ' + df['date_range'].astype(str) + ' , an invoice from ' + df['canonical_vendor_id'].astype(str) +' was not recieved'
    
    df['glean_date'] = df['date_range']
    df['glean_text'] = df['gleans']
    df['glean_type'] = 'no_invoice_recieved'
    df['glean_location'] = 'vendor'
    df['invoice_id'] = 'None'
    
    df = df[['glean_date', 'glean_text', 'glean_type', 'glean_location', 'invoice_id', 'canonical_vendor_id']]
    
    return df
    
    
    
    



    

    
