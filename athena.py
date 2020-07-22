import io,sys,time,boto3,pandas as pd

class Athena:
    
    """
    this class can be used to connect athena and run SELECT queries 
    or CREATE VIEW | TABLE queries.
    
    Parameters
    -------------
    
    aws_access_key : str
        AWS Access key identity
        
    aws_secret_access_key(str) : str 
        AWS Access Secret Access key
    
    region : str              
        Region name by default ap-southeast-1
        
    
    """
    
    def __init__(self,aws_access_key,aws_secret_access_key,region='ap-southeast-1'):
        
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        
        # query output location
        self.output_location = 's3://some_bucket/some_folder'
        
        try:
            self.client = boto3.client('athena', 
                          region_name = region, 
                          aws_access_key_id = aws_access_key,
                          aws_secret_access_key= aws_secret_access_key )
        except Exception as e:
            print(e)
       
     
    def to_pandas(self,query,database='some_db_schema'):
        
        """ 
        executes athena queries and returns pandas dataframe.
        
        Parameters
        -------------
        query : str
        sql query string
        
        database: str
        optional,qualifier database, default value is 'some_db_schema'
        
        Returns:
        ---------------
        retruns the query result into pandas dataframe
    
        """
        
        # obtain query_id
        query_id = self.client.start_query_execution(QueryString = query,
                    QueryExecutionContext={'Database': database},
                      ResultConfiguration={'OutputLocation': self.output_location})['QueryExecutionId']
    
    
        # execute query
        query_status = None
        while query_status in ['QUEUED','RUNNING',None]:
            query_status = self.client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
            if query_status == 'FAILED' or query_status == 'CANCELLED':
                raise Exception('Query execution failed/cancelled')
        
            time.sleep(10)
        
        # get the data from s3 location
        s3 = boto3.resource('s3', 
                region_name = self.region, 
                aws_access_key_id = self.aws_access_key,
                aws_secret_access_key= self.aws_secret_access_key)

        bucket = 's3_bucket_'
        filepath = 'credit_risk/'+query_id+'.csv'
        res = s3.Bucket(bucket).Object(key= filepath).get()
        return pd.read_csv(io.BytesIO(res['Body'].read()), encoding = 'utf8')

    def create_view(self,query,database='some_db_schema'):
        
        """
        execute CREATE VIEW | TABLE operations and returns status
        
        Parameters
        -------------
        query : str
        sql query string
        
        database: str
        optional,qualifier database, default value is 'some_db_schema'
        
        Returns:
        --------------
        retruns status
        
        """
        
        query_id = self.client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': database}, 
                    ResultConfiguration={'OutputLocation': self.output_location})['QueryExecutionId']

        # get query execution status
        query_status = None
        while query_status in ['QUEUED','RUNNING',None]:
            query_status = self.client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
            if query_status == 'FAILED' or query_status == 'CANCELLED':
                raise Exception('Query execution failed/cancelled')
        
            time.sleep(10)
    
        print(query_status)  
