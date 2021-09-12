import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
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
        
    def read_sample_data(self, scenario_number):  
        return TestAccessLogsAnalyzer.sc.textFile('data/sample_data')
    
    def test_read_source_1(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        result = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        no_lines = result.count()
        no_lines_expected = 10
        
        self.assertIsInstance(result, RDD, f'Expected RDD type and received {type(result)} instead.')
        self.assertEqual(no_lines, no_lines_expected, f'Expected {no_lines_expected} lines to be read and got {no_lines} instead.')
        
    def test_read_source_2(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        with self.assertRaises(Py4JJavaError):
            access_log_analyzer.read_source(self.sc, 'data/non_existing_file').collect()
    
    def test_calculate_cleansing_accuracy_1(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        result = access_log_analyzer.calculate_cleansing_accuracy(rdd)
        result_expected = 100 - (1 / 10 * 100)
        
        self.assertEqual(result, result_expected, f'Expected an accuracy of {result_expected} and got {result} instead.')
    
    def test_get_rdd_valid_lines_1(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        no_lines = access_log_analyzer.get_rdd_valid_lines(rdd).count()
        no_lines_expected = 9
        
        self.assertEqual(no_lines, no_lines_expected, f'Expected {no_lines_expected} lines to be read and got {no_lines} instead.')
        
    def test_map_rdd_1(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        rdd = access_log_analyzer.get_rdd_valid_lines(rdd)
        
        result = access_log_analyzer.map_rdd(rdd)
        
        no_lines = result.count()
        no_lines_expected = 9
        
        no_groups_expected = 9
        no_groups_bool = all(len(line) == no_groups_expected for line in result.collect())
        
        self.assertEqual(no_lines, no_lines_expected, f'Expected {no_lines_expected} lines to be read and got {no_lines} instead.')
        self.assertTrue(no_groups_bool, f'All tuples must have a length of {no_groups_expected}.')
    
    def test_map_rdd_2(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        
        with self.assertRaises(Py4JJavaError):
            result = access_log_analyzer.map_rdd(rdd).collect()
            
    def test_get_k_most_frequent_for_each_day_1(self):
        access_log_analyzer = AccessLogAnalyzer(k = 3, dataset_url = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz')
        
        rdd = access_log_analyzer.read_source(self.sc, 'data/sample_data')
        rdd = access_log_analyzer.get_rdd_valid_lines(rdd)
        rdd = access_log_analyzer.map_rdd(rdd)
        
        result = access_log_analyzer.get_k_most_frequent_for_each_day(rdd)
        
        self.assertEqual(len(result), 2, f'Expected 2 tuples in the result and got {len(result)} instead.')
        self.assertTrue(all(type(x) == dict for x in result), f'All objects within the resulting tuple must be a dictionary.')
        
        expected_result = ({datetime.date(1995, 7, 1): [('199.120.110.21', 2), ('199.72.81.55', 1), ('unicomp6.unicomp.net', 1)], datetime.date(1995, 7, 2): [], datetime.date(1995, 7, 3): [('burger.letters.com', 2), ('205.212.115.106', 1), ('d104.aa.net', 1)], datetime.date(1995, 7, 4): []}, {datetime.date(1995, 7, 1): [('/history/apollo/', 1), ('/shuttle/countdown/', 1), ('/shuttle/missions/sts-73/mission-sts-73.html', 1)], datetime.date(1995, 7, 2): [], datetime.date(1995, 7, 3): [('/images/NASA-logosmall.gif', 1), ('/shuttle/countdown/video/livevideo.gif', 1), ('/shuttle/countdown/countdown.html', 1)], datetime.date(1995, 7, 4): []})
        
        self.assertEqual(result, expected_result, f'The result does not match with the expected.')
        
def run_all_test(test_case_class):

    # Create a TestSuite object
    test_suite = unittest.TestSuite()
    
    # Add the tests
    test = unittest.makeSuite(test_case_class)
    test_suite.addTest(test)

if __name__== '__main__':
    run_all_test(TestCaseClass)