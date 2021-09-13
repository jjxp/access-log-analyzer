import unittest
import datetime
import re
import warnings
import sys
import os
import logging

from py4j.protocol import Py4JJavaError

from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

import findspark

sys.path.append('../jobs')

from nasa_access_logs_analyzer import AccessLogAnalyzer

class TestAccessLogsAnalyzer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('py4j')
        cls.logger.setLevel(logging.ERROR)
        
        findspark.init()
        
        conf = SparkConf().setMaster('local')      
        cls.sc = SparkContext(conf=conf)
        
        cls.sc.addFile('../jobs/nasa_access_logs_analyzer.py')
        
        TestAccessLogsAnalyzer.sc = cls.sc
        TestAccessLogsAnalyzer.sql_ctx = SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
    
    def test_instantiate_main_class_1(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        self.assertIsInstance(access_log_analyzer, AccessLogAnalyzer, f'Expected AccessLogAnalyzer type and received {type(access_log_analyzer)} instead.')
    
    def test_read_source_1(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        result = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        no_lines = result.count()
        no_lines_expected = 10
        
        self.assertIsInstance(result, RDD, f'Expected RDD type and received {type(result)} instead.')
        self.assertEqual(no_lines, no_lines_expected, f'Expected {no_lines_expected} lines to be read and got {no_lines} instead.')
        
    def test_read_source_2(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        with self.assertRaises(Py4JJavaError):
            access_log_analyzer.read_source(self.sc, 'data/non_existing_file').collect()
    
    def test_calculate_cleansing_accuracy_1(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        result = access_log_analyzer.calculate_cleansing_accuracy(rdd)
        result_expected = 100 - (1 / 10 * 100)
        
        self.assertEqual(result, result_expected, f'Expected an accuracy of {result_expected} and got {result} instead.')
    
    def test_get_rdd_valid_lines_1(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        no_lines = access_log_analyzer.get_rdd_valid_lines(rdd).count()
        no_lines_expected = 9
        
        self.assertEqual(no_lines, no_lines_expected, f'Expected {no_lines_expected} lines to be read and got {no_lines} instead.')
        
    def test_map_rdd_1(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        rdd = access_log_analyzer.get_rdd_valid_lines(rdd)
        
        result = access_log_analyzer.map_rdd(rdd)
        
        no_lines = result.count()
        no_lines_expected = 9
        
        no_groups_expected = 11
        no_groups_bool = all(len(line) == no_groups_expected for line in result.collect())
        
        self.assertEqual(no_lines, no_lines_expected, f'Expected {no_lines_expected} lines to be read and got {no_lines} instead.')
        self.assertTrue(no_groups_bool, f'All tuples must have a length of {no_groups_expected}.')
    
    def test_map_rdd_2(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        with self.assertRaises(Py4JJavaError):
            result = access_log_analyzer.map_rdd(rdd).collect()
            
    def test_get_n_most_frequent_for_each_day_1(self):
        access_log_analyzer = AccessLogAnalyzer(n = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        rdd = access_log_analyzer.get_rdd_valid_lines(rdd)
        rdd = access_log_analyzer.map_rdd(rdd)
        
        result = access_log_analyzer.get_n_most_frequent_for_each_day(TestAccessLogsAnalyzer.sql_ctx, rdd)
        
        result[0].show()
        
        result[1].show()
        
        self.assertEqual(len(result), 2, f'Expected 2 tuples in the result and got {len(result)} instead.')
        self.assertTrue(all(type(x) == DataFrame for x in result), f'All objects within the resulting tuple must be a DataFrame.')
        
        no_rows_result = result[0].count()
        expected_no_rows_result = 3 * 2 # n = 3 * distinct number of days = 2
        
        self.assertEqual(no_rows_result, expected_no_rows_result, f'Expected {expected_no_rows_result} rows in the result and got {no_rows_result} instead.')
        
        most_frequent_hosts = result[0].filter(F.col('count') == 2).collect()
        most_frequent_host_day_1 = most_frequent_hosts[0]['host']
        most_frequent_host_day_3 = most_frequent_hosts[1]['host']
        
        expected_most_frequent_host_day_1 = '199.120.110.21'
        expected_most_frequent_host_day_3 = 'burger.letters.com'
        
        self.assertEqual(most_frequent_host_day_1, expected_most_frequent_host_day_1, f'Expected {expected_most_frequent_host_day_1} for the most frequent host in day 1 and got {most_frequent_host_day_1} instead.')
        self.assertEqual(most_frequent_host_day_3, expected_most_frequent_host_day_3, f'Expected {expected_most_frequent_host_day_3} for the most frequent host in day 1 and got {most_frequent_host_day_3} instead.')
        
def run_all_test(test_case_class):

    # Create a TestSuite object
    test_suite = unittest.TestSuite()
    
    # Add the tests
    test = unittest.makeSuite(test_case_class)
    test_suite.addTest(test)

if __name__== '__main__':
    run_all_test(TestCaseClass)