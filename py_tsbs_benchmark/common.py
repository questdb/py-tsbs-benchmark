import requests
import time
import logging

# Set up logging
logger = logging.getLogger(__name__)


class CpuTable:
    """Helper class for managing QuestDB 'cpu' table operations via HTTP API.
    
    Provides methods to create, drop, and query the cpu table used in
    benchmarking. Uses QuestDB's HTTP query interface for table operations.
    """
    
    def __init__(self, host, port):
        """Initialize CpuTable with connection parameters.
        
        Args:
            host (str): QuestDB server hostname
            port (int): QuestDB HTTP port (typically 9000)
        """
        self.host = host
        self.port = port

    def _request(self, sql):
        """Execute SQL query via QuestDB HTTP API.
        
        Args:
            sql (str): SQL query to execute
            
        Returns:
            dict: JSON response from QuestDB server
            
        Raises:
            requests.RequestException: If HTTP request fails
            ValueError: If response is not valid JSON
        """
        try:
            logger.debug(f"Executing SQL: {sql[:100]}...")
            response = requests.get(
                f'http://{self.host}:{self.port}/exec',
                params={'query': sql},
                timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise

    def drop(self):
        """Drop the cpu table if it exists.
        
        Returns:
            bool: True if table was dropped, False if it didn't exist
            
        Raises:
            RuntimeError: If drop operation fails
        """
        try:
            logger.info("Attempting to drop cpu table")
            response = self._request('drop table cpu')
            if response.get('ddl') == 'OK':
                print('Dropped table cpu')
                logger.info("Successfully dropped cpu table")
                return True
            elif response.get('error', '').startswith('table does not exist'):
                print('Table cpu does not exist')
                logger.info("Table cpu does not exist, nothing to drop")
                return False
            else:
                error_msg = f'Failed to drop table cpu: {response}'
                logger.error(error_msg)
                raise RuntimeError(error_msg)
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            raise

    def create(self):
        """Create the cpu table with TSBS schema.
        
        Creates table with 10 symbol columns, 10 double columns, and
        a timestamp column. Sets up partitioning by day.
        
        Raises:
            RuntimeError: If table creation fails
        """
        try:
            logger.info("Creating cpu table with TSBS schema")
            symbol_cols = [
                'hostname', 'region', 'datacenter', 'rack', 'os', 'arch',
                'team', 'service', 'service_version', 'service_environment']
            double_cols = [
                'usage_user', 'usage_system', 'usage_idle', 'usage_nice',
                'usage_iowait', 'usage_irq', 'usage_softirq', 'usage_steal',
                'usage_guest', 'usage_guest_nice']
            sql = f'''
                create table cpu (
                    {', '.join(f'{col} symbol' for col in symbol_cols)},
                    {', '.join(f'{col} double' for col in double_cols)},
                    timestamp timestamp)
                        timestamp(timestamp)
                        partition by day
                '''
            response = self._request(sql)
            if response.get('ddl') == 'OK':
                print('Created table cpu')
                logger.info("Successfully created cpu table")
            else:
                error_msg = f'Failed to create table cpu: {response}'
                logger.error(error_msg)
                raise RuntimeError(error_msg)
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def get_row_count(self):
        """Get the current number of rows in the cpu table.
        
        Returns:
            int: Number of rows in the cpu table
        """
        response = self._request('select count(*) from cpu')
        return response['dataset'][0][0]

    def block_until_rowcount(self, target_count, timeout=30.0):
        """Block until the table reaches the target row count.
        
        Polls the table row count until it matches the target, with timeout.
        Used for validation after data ingestion.
        
        Args:
            target_count (int): Expected number of rows
            timeout (float): Maximum time to wait in seconds
            
        Raises:
            RuntimeError: If timeout is reached or row count exceeds target
        """
        t0 = time.monotonic()
        while True:
            row_count = self.get_row_count()
            if row_count == target_count:
                return
            elif row_count > target_count:
                raise RuntimeError(
                    f'Row count {row_count} exceeds target {target_count}')
            if time.monotonic() - t0 > timeout:
                raise RuntimeError(
                    f'Timed out waiting for row count to reach {target_count}')
            time.sleep(0.1)
