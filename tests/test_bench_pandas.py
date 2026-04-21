"""Unit tests for bench_pandas module."""

import unittest
import pandas as pd
import numpy as np

from py_tsbs_benchmark.bench_pandas import (
    _clip_add, gen_dataframe, chunk_up_dataframe,
    assign_dfs_to_workers, sanity_check_split, sanity_check_split2
)


class TestBenchPandas(unittest.TestCase):
    """Test cases for bench_pandas module."""

    def test_clip_add_normal_values(self):
        """Test _clip_add with normal values."""
        # Test values within range
        result = _clip_add(30.0, 40.0)
        self.assertEqual(result, 70.0)

    def test_clip_add_upper_bound(self):
        """Test _clip_add with values exceeding upper bound."""
        result = _clip_add(60.0, 50.0)
        self.assertEqual(result, 100.0)

    def test_clip_add_lower_bound(self):
        """Test _clip_add with values below lower bound."""
        result = _clip_add(-10.0, -5.0)
        self.assertEqual(result, 0.0)

    def test_clip_add_exact_bounds(self):
        """Test _clip_add with exact boundary values."""
        self.assertEqual(_clip_add(0.0, 0.0), 0.0)
        self.assertEqual(_clip_add(50.0, 50.0), 100.0)

    def test_gen_dataframe_structure(self):
        """Test that gen_dataframe creates correct structure."""
        # Arrange
        seed = 12345
        row_count = 1000
        scale = 100
        
        # Act
        df = gen_dataframe(seed, row_count, scale)
        
        # Assert
        self.assertEqual(len(df), row_count)
        self.assertEqual(df.index.name, 'cpu')
        
        # Check symbol columns exist
        symbol_cols = [
            'hostname', 'region', 'datacenter', 'rack', 'os', 'arch',
            'team', 'service', 'service_version', 'service_environment'
        ]
        for col in symbol_cols:
            self.assertIn(col, df.columns)
            self.assertEqual(str(df[col].dtype), 'string')
            
        # Check numeric columns exist
        numeric_cols = [
            'usage_user', 'usage_system', 'usage_idle', 'usage_nice',
            'usage_iowait', 'usage_irq', 'usage_softirq', 'usage_steal',
            'usage_guest', 'usage_guest_nice'
        ]
        for col in numeric_cols:
            self.assertIn(col, df.columns)
            self.assertEqual(df[col].dtype, np.float64)
            
        # Check timestamp column
        self.assertIn('timestamp', df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['timestamp']))

    def test_gen_dataframe_reproducibility(self):
        """Test that gen_dataframe is reproducible with same seed."""
        # Arrange
        seed = 54321
        row_count = 100
        scale = 50
        
        # Act
        df1 = gen_dataframe(seed, row_count, scale)
        df2 = gen_dataframe(seed, row_count, scale)
        
        # Assert
        pd.testing.assert_frame_equal(df1, df2)

    def test_gen_dataframe_hostnames_scale(self):
        """Test that hostname generation respects scale parameter."""
        # Arrange
        seed = 99999
        row_count = 1000
        scale = 10
        
        # Act
        df = gen_dataframe(seed, row_count, scale)
        
        # Assert
        unique_hostnames = df['hostname'].unique()
        self.assertEqual(len(unique_hostnames), scale)
        
        # Check hostname pattern
        for hostname in unique_hostnames:
            self.assertTrue(hostname.startswith('host_'))

    def test_chunk_up_dataframe(self):
        """Test DataFrame chunking."""
        # Arrange
        df = pd.DataFrame({'a': range(100), 'b': range(100, 200)})
        chunk_size = 30
        
        # Act
        chunks = chunk_up_dataframe(df, chunk_size)
        
        # Assert
        self.assertEqual(len(chunks), 4)  # 100 / 30 = 3.33, so 4 chunks
        self.assertEqual(len(chunks[0]), 30)
        self.assertEqual(len(chunks[1]), 30)
        self.assertEqual(len(chunks[2]), 30)
        self.assertEqual(len(chunks[3]), 10)  # Last chunk has remainder

    def test_assign_dfs_to_workers(self):
        """Test DataFrame assignment to workers."""
        # Arrange
        dfs = [pd.DataFrame({'a': [i]}) for i in range(10)]
        workers = 3
        
        # Act
        result = assign_dfs_to_workers(dfs, workers)
        
        # Assert
        self.assertEqual(len(result), workers)
        # Check round-robin assignment
        self.assertEqual(len(result[0]), 4)  # indices 0, 3, 6, 9
        self.assertEqual(len(result[1]), 3)  # indices 1, 4, 7
        self.assertEqual(len(result[2]), 3)  # indices 2, 5, 8

    def test_sanity_check_split_valid(self):
        """Test sanity check with valid split."""
        # Arrange
        df = pd.DataFrame({'a': range(10), 'b': range(10, 20)})
        chunks = [df.iloc[:5], df.iloc[5:]]
        
        # Act & Assert (should not raise)
        sanity_check_split(df, chunks)

    def test_sanity_check_split_invalid_length(self):
        """Test sanity check with invalid split length."""
        # Arrange
        df = pd.DataFrame({'a': range(10), 'b': range(10, 20)})
        chunks = [df.iloc[:3], df.iloc[5:]]  # Missing rows 3-4
        
        # Act & Assert
        with self.assertRaises(AssertionError):
            sanity_check_split(df, chunks)

    def test_sanity_check_split2_valid(self):
        """Test sanity check for worker assignment."""
        # Arrange
        df = pd.DataFrame({
            'a': range(6),
            'timestamp': pd.date_range('2023-01-01', periods=6, freq='1H')
        })
        # Split into 2 workers, 3 chunks each
        dfs_by_worker = [
            [df.iloc[:2], df.iloc[4:5]],  # Worker 0: rows 0-1, 4
            [df.iloc[2:4], df.iloc[5:6]]  # Worker 1: rows 2-3, 5
        ]
        
        # Act & Assert (should not raise)
        sanity_check_split2(df, dfs_by_worker)

    def test_sanity_check_split2_invalid(self):
        """Test sanity check for invalid worker assignment."""
        # Arrange
        df = pd.DataFrame({
            'a': range(6),
            'timestamp': pd.date_range('2023-01-01', periods=6, freq='1H')
        })
        # Invalid split - missing a row
        dfs_by_worker = [
            [df.iloc[:2]],  # Worker 0: rows 0-1
            [df.iloc[2:5]]  # Worker 1: rows 2-4 (missing row 5)
        ]
        
        # Act & Assert
        with self.assertRaises(AssertionError):
            sanity_check_split2(df, dfs_by_worker)


class TestDataFrameGeneration(unittest.TestCase):
    """Additional tests for DataFrame generation edge cases."""

    def test_small_scale_large_rowcount(self):
        """Test with small scale and large row count."""
        # Arrange
        seed = 1111
        row_count = 1000
        scale = 5  # Small number of hosts
        
        # Act
        df = gen_dataframe(seed, row_count, scale)
        
        # Assert
        self.assertEqual(len(df), row_count)
        unique_hostnames = df['hostname'].unique()
        self.assertEqual(len(unique_hostnames), scale)
        
        # Each hostname should appear multiple times
        hostname_counts = df['hostname'].value_counts()
        self.assertTrue(all(count > 1 for count in hostname_counts))

    def test_large_scale_small_rowcount(self):
        """Test with large scale and small row count."""
        # Arrange
        seed = 2222
        row_count = 50
        scale = 100  # More hosts than rows
        
        # Act
        df = gen_dataframe(seed, row_count, scale)
        
        # Assert
        self.assertEqual(len(df), row_count)
        # Should have at most row_count unique hostnames
        unique_hostnames = df['hostname'].unique()
        self.assertLessEqual(len(unique_hostnames), row_count)

    def test_cpu_usage_values_realistic(self):
        """Test that CPU usage values are within realistic bounds."""
        # Arrange
        seed = 3333
        row_count = 1000
        scale = 100
        
        # Act
        df = gen_dataframe(seed, row_count, scale)
        
        # Assert - CPU usage values should be >= 0 and <= 100
        usage_cols = [col for col in df.columns if col.startswith('usage_')]
        for col in usage_cols:
            values = df[col]
            self.assertTrue(all(val >= 0.0 for val in values),
                            f"Found negative values in {col}")
            self.assertTrue(all(val <= 100.0 for val in values),
                            f"Found values > 100 in {col}")


if __name__ == '__main__':
    unittest.main()
