'''
This is a python coding challenge proposed by secureworks.
The purpose of this job is to download an access log dataset from NASA and compute the
n-most-frequent visitors and URLs for each day of the trace using Spark.

@author     = 'Javier García Calvo'

@version    = '1.0'

@maintainer = ['Javier García Calvo']

@status     = 'Stable'

@creation_date = 12/09/2021

@last_modification = 15/09/2021

'''

# Import all required dependencies
import sys
import re
import logging
import shutil
import urllib.request as request
from contextlib import closing
from datetime import datetime
from calendar import Calendar, monthrange
import findspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

class AccessLogAnalyzer():
    '''
    This class contains all the required logics to download and perform analytical operations over a NASA access log dataset.
    
    Args:
        n (int): integer, greater than zero, that will indicate how many most-frequent distinct values we want to obtain as a result
        dataset_url (str): URL of the dataset that we want to download
    '''
    def __init__(self, **kwargs):
        # Define a logger
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('access_log_analyzer')
        
        # Perform checks to test whether the parameters have the expected values
        if not type(kwargs['n']) == int:
            raise ValueError('The "n" parameter does not match the expected datatype (int)')
        
        if not kwargs['n'] > 0:
            raise ValueError('The "n" parameter must be greater than zero')
        
        if not type(kwargs['dataset_url']) == str:
            raise ValueError('The "dataset_url" parameter does not match the expected datatype (str)')
        
        if not re.match('^(s?ftp:\/\/)[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$', kwargs['dataset_url']):
            raise ValueError('The "dataset_url" parameter does not match a valid FTP URL')
        
        # Assign external arguments to class attributes
        self.n = kwargs['n']
        self.dataset_url = kwargs['dataset_url']
        
        # Regex for cleaning and extracting groups from the logs
        host = '^(\S+) '
        identity_remote = '(\S+) '
        identity_local = '(\S+) '
        date = '\[([\w/]+)'
        hour = '([:\d]+) '
        timezone = '([+\-]\d{4})\] "'
        request_method = '(\S+) '
        resource = '(\S+) *'
        protocol = '(\S+)? *" '
        status_code = '(\d{3}) '
        bytes_returned = '(\S+)'
        self.regex = host + identity_remote + identity_local + date + hour + timezone + request_method + resource + protocol + status_code + bytes_returned

        self.logger.info(f'AccessLogAnalyzer class is ready!')

    def create_spark_context(self): # pragma: no cover
        '''
        Creates and returns an instance of the SparkContext and SQLContext
        
        Returns:
            tuple(SparkContext, SQLContext): Tuple containing SparkContext and SQLContext
        '''
        findspark.init()

        spark = SparkSession.builder.master('local').\
        appName('nasa-access-log-analyzer').\
        config('spark.driver.bindAddress', 'localhost').\
        config('spark.ui.port', '4050').\
        getOrCreate()
        
        sc = spark.sparkContext
        sql_context = SQLContext(sc)
        
        return (sc, sql_context)
    
    def download_access_logs(self): # pragma: no cover
        '''
        Connects to the FTP repository provided in the arguments to this job, and downloads it
        
        Returns:
            str: String value of the downloaded filename
        '''
        # Retrieve the access log name, getting the last part of the URL (already verified with a regex)
        access_log_name = self.dataset_url.split('/')[-1]
        
        with closing(request.urlopen(self.dataset_url)) as r:
            with open(access_log_name, 'wb') as f:
                shutil.copyfileobj(r, f)
        
        return access_log_name
    
    def read_source(self, sc, source_name):
        '''
        Reads the source access logs and creates a Spark RDD out of it
        Assumes that the source data is coming in a format that the textFile function of SparkContext will be able to parse
        
        Args:
            sc (SparkContext): An instance of a SparkContext
            source_name (str): URL or location of the dataset that we want to download
        
        Returns:
            RDD: A Spark RDD pointing to the access logs data
        '''
        rdd = sc.textFile(source_name)
        
        return rdd
    
    def check_log_line(self, line):
        '''
        Checks whether, out of a particular line, the validity (regex compliance) of such line
        
        Args:
            line (?): Argument of a lambda expression performed over a RDD
        
        Returns:
            tuple(?, bool): A tuple that will contain the line and a boolean that indicates whether the line is valid or not
        '''
        match = re.search(self.regex, line)

        if match is None:
            return (line, False)

        return (line, True)

    def map_log_line(self, line):
        '''
        Cleansing function that will, out of a particular line, map it according to the regex groups
        
        Args:
            line (?): Argument of a lambda expression performed over a RDD
        
        Returns:
            tuple: A tuple containing the groups that match the regex expression
        '''
        match = re.search(self.regex, line)

        return match.groups()
    
    def calculate_cleansing_accuracy(self, rdd):
        '''
        Calculates the accuracy of the cleansing process performed over the source data.
        Accuracy is calculated with the following expression: (100 - (failed_lines / total_lines * 100)).
        It indicates the percentage of lines that the job has been able to parse successfully.
        
        Args:
            rdd (RDD): The RDD over which we want to perform the operation
        
        Returns:
            float: Float number for the cleansing accuracy
        '''
        # Obtain the total number of lines
        _total_no_lines = rdd.count()
        
        # Obtain the number of lines that failed parsing
        _no_failed_lines_parsing = rdd.map(lambda line: self.check_log_line(line)).filter(lambda line: not line[1]).count()
        
        # Percentage of failed lines over total number of lines
        cleansing_accuracy = (100 - (_no_failed_lines_parsing / _total_no_lines * 100))

        self.logger.info(f'Failed to parse {_no_failed_lines_parsing} out of {_total_no_lines}')
        self.logger.info(f'Accuracy of the cleansing process is {cleansing_accuracy:.2f}%')
        
        return cleansing_accuracy
    
    def get_rdd_valid_lines(self, rdd):
        '''
        Receives a RDD and returns only the lines that are valid, according to the regex specifications
        
        Args:
            rdd (RDD): The RDD over which we want to perform the operation
        
        Returns:
            RDD: Filtered RDD with only valid lines according to the regex provided
        '''
        return rdd.map(lambda line: self.check_log_line(line)).filter(lambda line: line[1]).map(lambda line: line[0])
    
    def map_rdd(self, rdd):
        '''
        Receives a RDD and maps its lines to the regex groups specified
        
        Args:
            rdd (RDD): The RDD over which we want to perform the operation
        
        Returns:
            RDD: Mapped RDD according to the regex groups specified
        '''
        return rdd.map(lambda line: self.map_log_line(line))
    
    def get_n_most_frequent_for_columns(self, df, col_a, col_b):
        '''
        Receives a DataFrame and, grouping by the specified columns, calculates the number of rows for the second column.
        After that, it performs a window function that assigns a row number over the first column (partition key) and orders it in descending order.
        Then, it performs a filter operation and keeps only the values for the 'row_number' column that are minor or equal to 'N'.
        Afterwards, it orders the data in ascending order for the first column and 'row_number' column. Then, it drops the 'row_number' column.
        In this way, it obtains the n-most-frequent values of the second column and their frequence for each value of the first column.
        
        Args:
            df (df): The DataFrame over which we want to perform the operation
            col_a (str): The string column name on which we will partition over
            col_b (str): The string column name on which we will calculate the n-most-frequent values
        
        Returns:
            DataFrame: Parsed DataFrame with the n-most-frequent values of the second column and their frequence for each value of the first column
        '''
        if not col_a in df.columns:
            raise ValueError(f'{col_a} is not present in the DataFrame columns.')
        
        if not col_b in df.columns:
            raise ValueError(f'{col_b} is not present in the DataFrame columns.')
            
        return df.groupBy(F.col(col_a), F.col(col_b)).agg(F.count(F.col(col_b)).alias('count')).withColumn('row_number', F.row_number().over(Window.partitionBy(F.col(col_a)).orderBy(F.desc('count')))).filter(F.col('row_number') <= self.n).orderBy(F.asc(col_a), F.asc('row_number')).drop('row_number')
    
    def get_n_most_frequent_for_each_day(self, sql_context, rdd, sampling_ratio = 0.1):
        '''
        Calculates the n-most-frequent visitors and URLs for each day in the trace
        
        Args:
            sql_context (SQLContext): An instance of the SQLContext
            rdd (RDD): The RDD over which we want to perform the operation
            sampling_ratio (float): [optional - defaults to 0.1] The sampling ratio of selected rows over the total to infer the datatypes when creating the DataFrame
        
        Returns:
            tuple(DataFrame, DataFrame): A tuple containing first a DataFrame with the n most frequent visitors, and a second with the n most frequent urls
        '''
        # Create a dataframe out of a processed RDD and define the schema. Sampling ratio is needed to infer the datatypes
        _df = sql_context.createDataFrame(rdd, schema = ['host', 'identity_remote', 'identity_local', 'date', 'time', 'timezone', 'request_method', 'resource', 'protocol', 'status_code', 'bytes_returned'], samplingRatio = sampling_ratio)
        
        most_frequent_visitors = self.get_n_most_frequent_for_columns(_df, 'date', 'host')
        most_frequent_urls = self.get_n_most_frequent_for_columns(_df, 'date', 'resource')
        
        return (most_frequent_visitors, most_frequent_urls)

if __name__== "__main__" : # pragma: no cover
    # Start off by creating an instance of the AccessLogAnalyzer class, passing the sys arguments as a parameter
    log_analyzer = AccessLogAnalyzer(n = int(sys.argv[1]), dataset_url = sys.argv[2])
    
    # Download the logs
    logs_name = log_analyzer.download_access_logs()
    
    # Create the Spark Context and SQLContext
    (sc, sql_context) = log_analyzer.create_spark_context()
    
    # Read the source data
    rdd = log_analyzer.read_source(sc, logs_name)
    
    # Calculate the cleansing accuracy of the process
    log_analyzer.calculate_cleansing_accuracy(rdd)
    
    # Filter the RDD and get only the valid lines that match the regex pattern
    rdd = log_analyzer.get_rdd_valid_lines(rdd)
    
    # Map the RDD and obtain groups from each line
    rdd = log_analyzer.map_rdd(rdd)
    
    # Get the most frequent visitors and urls for each day of the trace
    (most_frequent_visitors, most_frequent_urls) = log_analyzer.get_n_most_frequent_for_each_day(sql_context, rdd)

    # Write the output to CSV files and join all the partitions in a single file
    most_frequent_visitors.coalesce(1).write.csv(f'{log_analyzer.n}_most_frequent_visitors.csv')
    most_frequent_urls.coalesce(1).write.csv(f'{log_analyzer.n}_most_frequent_urls.csv')
    
    sc.stop()