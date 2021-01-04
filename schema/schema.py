#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  3 21:58:16 2021

@author: jeremiahjohnson
"""

from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType

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