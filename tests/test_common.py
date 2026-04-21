"""Unit tests for common module."""

import unittest
from unittest.mock import Mock, patch
import requests

from py_tsbs_benchmark.common import CpuTable


class TestCpuTable(unittest.TestCase):
    """Test cases for CpuTable class."""

    def setUp(self):
        """Set up test fixtures."""
        self.host = 'localhost'
        self.port = 9000
        self.cpu_table = CpuTable(self.host, self.port)

    @patch('py_tsbs_benchmark.common.requests.get')
    def test_request_success(self, mock_get):
        """Test successful HTTP request."""
        # Arrange
        mock_response = Mock()
        mock_response.json.return_value = {'ddl': 'OK'}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Act
        result = self.cpu_table._request('SELECT 1')
        
        # Assert
        self.assertEqual(result, {'ddl': 'OK'})
        mock_get.assert_called_once_with(
            f'http://{self.host}:{self.port}/exec',
            params={'query': 'SELECT 1'},
            timeout=30
        )

    @patch('py_tsbs_benchmark.common.requests.get')
    def test_request_http_error(self, mock_get):
        """Test HTTP request failure."""
        # Arrange
        mock_get.side_effect = requests.RequestException("Connection error")
        
        # Act & Assert
        with self.assertRaises(requests.RequestException):
            self.cpu_table._request('SELECT 1')

    @patch('py_tsbs_benchmark.common.requests.get')
    def test_request_json_error(self, mock_get):
        """Test JSON parsing error."""
        # Arrange
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        # Act & Assert
        with self.assertRaises(ValueError):
            self.cpu_table._request('SELECT 1')

    def test_drop_table_success(self):
        """Test successful table drop."""
        # Arrange
        with patch.object(self.cpu_table, '_request') as mock_request:
            mock_request.return_value = {'ddl': 'OK'}
            
            # Act
            result = self.cpu_table.drop()
            
            # Assert
            self.assertTrue(result)
            mock_request.assert_called_once_with('drop table cpu')

    def test_drop_table_not_exists(self):
        """Test drop table when table doesn't exist."""
        # Arrange
        with patch.object(self.cpu_table, '_request') as mock_request:
            mock_request.return_value = {'error': 'table does not exist'}
            
            # Act
            result = self.cpu_table.drop()
            
            # Assert
            self.assertFalse(result)

    def test_drop_table_failure(self):
        """Test table drop failure."""
        # Arrange
        with patch.object(self.cpu_table, '_request') as mock_request:
            mock_request.return_value = {'error': 'Some other error'}
            
            # Act & Assert
            with self.assertRaises(RuntimeError):
                self.cpu_table.drop()

    def test_create_table_success(self):
        """Test successful table creation."""
        # Arrange
        with patch.object(self.cpu_table, '_request') as mock_request:
            mock_request.return_value = {'ddl': 'OK'}
            
            # Act
            self.cpu_table.create()
            
            # Assert
            mock_request.assert_called_once()
            call_args = mock_request.call_args[0][0]
            self.assertIn('create table cpu', call_args)
            self.assertIn('hostname symbol', call_args)
            self.assertIn('usage_user double', call_args)
            self.assertIn('timestamp timestamp', call_args)

    def test_create_table_failure(self):
        """Test table creation failure."""
        # Arrange
        with patch.object(self.cpu_table, '_request') as mock_request:
            mock_request.return_value = {'error': 'Creation failed'}
            
            # Act & Assert
            with self.assertRaises(RuntimeError):
                self.cpu_table.create()

    def test_get_row_count(self):
        """Test getting row count."""
        # Arrange
        with patch.object(self.cpu_table, '_request') as mock_request:
            mock_request.return_value = {'dataset': [[12345]]}
            
            # Act
            count = self.cpu_table.get_row_count()
            
            # Assert
            self.assertEqual(count, 12345)
            mock_request.assert_called_once_with('select count(*) from cpu')

    @patch('py_tsbs_benchmark.common.time.sleep')
    @patch('py_tsbs_benchmark.common.time.monotonic')
    def test_block_until_rowcount_success(self, mock_time, mock_sleep):
        """Test successful blocking until row count reached."""
        # Arrange
        mock_time.side_effect = [0, 1]  # Start time, end time
        with patch.object(self.cpu_table, 'get_row_count') as mock_count:
            mock_count.return_value = 1000
            
            # Act
            self.cpu_table.block_until_rowcount(1000, timeout=30.0)
            
            # Assert
            mock_count.assert_called_once()

    @patch('py_tsbs_benchmark.common.time.sleep')
    @patch('py_tsbs_benchmark.common.time.monotonic')
    def test_block_until_rowcount_timeout(self, mock_time, mock_sleep):
        """Test timeout when waiting for row count."""
        # Arrange
        mock_time.side_effect = [0, 35]  # Start time, timeout exceeded
        with patch.object(self.cpu_table, 'get_row_count') as mock_count:
            mock_count.return_value = 500  # Less than target
            
            # Act & Assert
            with self.assertRaises(RuntimeError) as cm:
                self.cpu_table.block_until_rowcount(1000, timeout=30.0)
            
            self.assertIn('Timed out', str(cm.exception))

    @patch('py_tsbs_benchmark.common.time.sleep')
    @patch('py_tsbs_benchmark.common.time.monotonic')
    def test_block_until_rowcount_exceeds(self, mock_time, mock_sleep):
        """Test when row count exceeds target."""
        # Arrange
        mock_time.side_effect = [0, 1]
        with patch.object(self.cpu_table, 'get_row_count') as mock_count:
            mock_count.return_value = 1500  # More than target
            
            # Act & Assert
            with self.assertRaises(RuntimeError) as cm:
                self.cpu_table.block_until_rowcount(1000, timeout=30.0)
            
            self.assertIn('exceeds target', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
