"""
Data deduplication transformations.
Identifies and removes duplicate records based on business keys.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, max as spark_max
from pyspark.sql.window import Window
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class Deduplication:
    """Data deduplication operations."""
    
    @staticmethod
    def remove_duplicates_by_key(df: DataFrame, key_columns: List[str],
                                 timestamp_column: Optional[str] = None,
                                 keep: str = 'latest') -> DataFrame:
        """
        Remove duplicates based on business key columns.
        
        Args:
            df: Input DataFrame
            key_columns: List of columns that form the business key
            timestamp_column: Optional timestamp column to determine which record to keep
            keep: 'latest' or 'earliest' - which record to keep when duplicates found
            
        Returns:
            DataFrame with duplicates removed
        """
        initial_count = df.count()
        
        if timestamp_column and timestamp_column in df.columns:
            # Use timestamp to determine which record to keep
            if keep == 'latest':
                window_spec = Window.partitionBy(key_columns).orderBy(col(timestamp_column).desc())
            else:
                window_spec = Window.partitionBy(key_columns).orderBy(col(timestamp_column).asc())
            
            df_deduped = df.withColumn(
                'row_num',
                row_number().over(window_spec)
            ).filter(col('row_num') == 1).drop('row_num')
        else:
            # Simple deduplication - keep first occurrence
            df_deduped = df.dropDuplicates(subset=key_columns)
        
        final_count = df_deduped.count()
        duplicate_count = initial_count - final_count
        
        logger.info(f"Removed {duplicate_count} duplicate records based on key columns: {key_columns}")
        
        return df_deduped
    
    @staticmethod
    def identify_duplicates(df: DataFrame, key_columns: List[str]) -> DataFrame:
        """
        Identify duplicate records without removing them.
        Adds a 'duplicate_count' column showing how many times each key appears.
        
        Args:
            df: Input DataFrame
            key_columns: List of columns that form the business key
            
        Returns:
            DataFrame with duplicate_count column added
        """
        from pyspark.sql.functions import count
        
        # Count occurrences of each key
        key_counts = df.groupBy(key_columns).agg(
            count('*').alias('duplicate_count')
        )
        
        # Join back to original DataFrame
        df_with_counts = df.join(key_counts, on=key_columns, how='left')
        
        return df_with_counts
    
    @staticmethod
    def get_duplicate_summary(df: DataFrame, key_columns: List[str]) -> dict:
        """
        Get summary statistics about duplicates.
        
        Args:
            df: Input DataFrame
            key_columns: List of columns that form the business key
            
        Returns:
            Dictionary with duplicate statistics
        """
        total_records = df.count()
        
        # Count duplicates
        duplicate_df = df.groupBy(key_columns).count().filter(col('count') > 1)
        duplicate_keys = duplicate_df.count()
        duplicate_records = duplicate_df.agg({'count': 'sum'}).collect()[0][0] - duplicate_keys
        
        summary = {
            'total_records': total_records,
            'unique_keys': total_records - duplicate_records,
            'duplicate_keys': duplicate_keys,
            'duplicate_records': duplicate_records,
            'duplicate_percentage': (duplicate_records / total_records * 100) if total_records > 0 else 0
        }
        
        logger.info(f"Duplicate summary: {summary}")
        
        return summary
    
    @staticmethod
    def remove_duplicates_by_transaction_id(df: DataFrame, 
                                           transaction_id_col: str = 'transaction_id',
                                           timestamp_col: Optional[str] = None) -> DataFrame:
        """
        Remove duplicates by transaction_id (common use case).
        
        Args:
            df: Input DataFrame
            transaction_id_col: Name of transaction ID column
            timestamp_col: Optional timestamp column for keeping latest
            
        Returns:
            DataFrame with duplicate transactions removed
        """
        return Deduplication.remove_duplicates_by_key(
            df, 
            key_columns=[transaction_id_col],
            timestamp_column=timestamp_col,
            keep='latest'
        )
    
    @staticmethod
    def remove_duplicates_by_customer_timestamp(df: DataFrame,
                                               customer_id_col: str = 'customer_id',
                                               timestamp_col: str = 'sale_date') -> DataFrame:
        """
        Remove duplicates by customer_id and timestamp (common use case).
        
        Args:
            df: Input DataFrame
            customer_id_col: Name of customer ID column
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with duplicates removed
        """
        return Deduplication.remove_duplicates_by_key(
            df,
            key_columns=[customer_id_col, timestamp_col],
            timestamp_column=timestamp_col,
            keep='latest'
        )

