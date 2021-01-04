#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  3 20:11:49 2021

@author: jeremiahjohnson
"""

from pyspark.sql import SparkSession
from data_transform.glean_generators import glean_1, glean_2, glean_3, glean_4
from schema.schema import invoice_schema, line_item_schema
import pandas as pd



def main():
    spark = SparkSession.builder.appName('glean').getOrCreate()

    invoices = spark.read.csv('data/invoice.csv', schema = invoice_schema, header = True)

    line_items = spark.read.csv('data/line_item.csv', schema = line_item_schema, header = True)
    
    glean1 = glean_1(invoices)
    glean2 = glean_2(invoices, line_items)
    glean3 = glean_3(invoices)
    glean4 = glean_4(invoices)

    print(glean1.head(), glean2.head(), glean3.head(), glean4.head())
    
    g1 = glean1.toPandas()
    g2 = glean2.toPandas()
    g3 = glean3.toPandas()

    df = pd.concat([g1, g2, g3, glean4])
    df = df[['glean_date', 'glean_text', 'glean_type', 'glean_location', 'invoice_id', 'canonical_vendor_id']]
    df.to_csv('gleans.csv')
    

if __name__ == '__main__':
    main()