import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import datetime
import re
import warnings
import sys
import os
import logging

from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf

import findspark

sys.path.append('../jobs/')

import nasa_access_logs_analyzer as testing_job

class TestAccessLogsAnalyzer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('py4j')
        cls.logger.setLevel(logging.ERROR)
        
        findspark.init()
        
        conf = SparkConf().setMaster("local")
        
        cls.sc = SparkContext(conf=conf)
        
        TestAccessLogsAnalyzer.sc = cls.sc
        TestAccessLogsAnalyzer.sql_ctx = SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        
    def read_sample_data(self, scenario_number):
        
        return TestAccessLogsAnalyzer.sc.textFile('data/sample_data')
    
class TestCase1(TestAccessLogsAnalyzer):
    def test_dim_class_generic_scenario(self):
        print(self.sc.version)
        
        print(self.read_sample_data(5).take(5))

def run_all_test(test_case_class):

    # Create a TestSuite object.
    test_suite = unittest.TestSuite()

    # Make all test function 
    test = unittest.makeSuite(test_case_class)
    test_suite.addTest(test)

if __name__== '__main__':
    run_all_test(TestCaseClass)