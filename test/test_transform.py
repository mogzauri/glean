#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: jeremiahjohnson
"""

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from data_transform.glean_generators import glean_1, glean_2, glean_3, glean_4
from schema.schema import invoice_schema, line_item_schema

pytestmark = pytest.mark.usefixtures("spark")


def assert_df_equals(actual_df, expected_df, sort_col):
    """
    This function helps compare dataframes and see if they are equal.
    :param actual_df: Spark DataFrame that is result of applying function to input df
    :param expected_df: Spark DataFrame with the expected results of the function
    :param sort_col: column to sort by - needed for assert_frame_equal (compares row to row)
    :return: raises an AssertionError if test fails
    """
    actual_df_pd = actual_df.toPandas().sort_values(by=sort_col).reset_index(drop=True)
    expected_df_pd = expected_df.toPandas().sort_values(by=sort_col).reset_index(drop=True)
    assert_frame_equal(actual_df_pd, expected_df_pd)
    

def test_glean_1(data):
    df = glean_1(data)
    print(df.toPandas().head(20))
        
def test_glean_2(data1, data2):
    df = glean_2(data)
    print(df.toPandas().head(20)) 
    
def test_glean_3(data):
    df = glean_3(data)
    print(df.toPandas().head(20))
    
def test_glean_4(data):
    df = glean_4(data)
    print(df.head(20))
