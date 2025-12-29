"""
Data cleansing transformations.
Handles null/invalid records, date standardization, text cleaning, and missing value handling.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, when, isnan, isnull, to_date, 
    col as spark_col, regexp_replace, coalesce, last
)
from pyspark.sql.types import StringType, DateType
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataCleansing:
    """Data cleansing operations."""
    
    @staticmethod
    def remove_null_records(df: DataFrame, columns: list = None) -> DataFrame:
        """
        Remove records with null values in specified columns.
        
        Args:
            df: Input DataFrame
            columns: List of columns to check for nulls. If None, checks all columns.
            
        Returns:
            Cleaned DataFrame
        """
        if columns is None:
            columns = df.columns
        
        initial_count = df.count()
        
        # Remove rows where all specified columns are null
        df_cleaned = df.dropna(subset=columns, how='all')
        
        final_count = df_cleaned.count()
        removed_count = initial_count - final_count
        
        logger.info(f"Removed {removed_count} records with null values in columns: {columns}")
        
        return df_cleaned
    
    @staticmethod
    def standardize_dates(df: DataFrame, date_columns: dict) -> DataFrame:
        """
        Standardize date formats in specified columns.
        
        Args:
            df: Input DataFrame
            date_columns: Dictionary mapping column names to date formats
                          e.g., {'sale_date': 'yyyy-MM-dd', 'order_date': 'MM/dd/yyyy'}
            
        Returns:
            DataFrame with standardized dates
        """
        df_cleaned = df
        
        for col_name, date_format in date_columns.items():
            if col_name in df.columns:
                # Try to parse date with specified format
                df_cleaned = df_cleaned.withColumn(
                    col_name,
                    to_date(col(col_name), date_format)
                )
                logger.info(f"Standardized date column '{col_name}' with format '{date_format}'")
        
        return df_cleaned
    
    @staticmethod
    def clean_text_fields(df: DataFrame, text_columns: list = None, 
                          trim_whitespace: bool = True, 
                          uppercase: bool = False) -> DataFrame:
        """
        Clean text fields by trimming whitespace and optionally converting to uppercase.
        
        Args:
            df: Input DataFrame
            text_columns: List of text columns to clean. If None, infers string columns.
            trim_whitespace: Whether to trim whitespace
            uppercase: Whether to convert to uppercase
            
        Returns:
            DataFrame with cleaned text fields
        """
        if text_columns is None:
            # Infer string columns
            text_columns = [
                field.name for field in df.schema.fields 
                if isinstance(field.dataType, StringType)
            ]
        
        df_cleaned = df
        
        for col_name in text_columns:
            if col_name in df.columns:
                if trim_whitespace:
                    df_cleaned = df_cleaned.withColumn(col_name, trim(col(col_name)))
                
                if uppercase:
                    df_cleaned = df_cleaned.withColumn(col_name, upper(col(col_name)))
        
        logger.info(f"Cleaned text fields: {text_columns}")
        
        return df_cleaned
    
    @staticmethod
    def handle_missing_values(df: DataFrame, fill_strategy: dict) -> DataFrame:
        """
        Handle missing values using specified strategies.
        
        Args:
            df: Input DataFrame
            fill_strategy: Dictionary mapping column names to fill strategies
                          e.g., {'sale_amount': 'zero', 'region': 'unknown', 'quantity': 'mean'}
                          Strategies: 'zero', 'mean', 'median', 'mode', 'forward_fill', 'backward_fill', 'constant'
            
        Returns:
            DataFrame with missing values handled
        """
        df_cleaned = df
        
        for col_name, strategy in fill_strategy.items():
            if col_name not in df.columns:
                continue
            
            if strategy == 'zero':
                df_cleaned = df_cleaned.fillna({col_name: 0})
            
            elif strategy == 'mean':
                mean_value = df.select(col(col_name)).agg({col_name: 'mean'}).collect()[0][0]
                df_cleaned = df_cleaned.fillna({col_name: mean_value})
            
            elif strategy == 'median':
                # Approximate median using percentiles
                median_value = df.approxQuantile(col_name, [0.5], 0.25)[0]
                df_cleaned = df_cleaned.fillna({col_name: median_value})
            
            elif strategy == 'forward_fill':
                from pyspark.sql.window import Window
                window = Window.orderBy(col_name).rowsBetween(Window.unboundedPreceding, Window.currentRow)
                df_cleaned = df_cleaned.withColumn(
                    col_name,
                    coalesce(col(col_name), last(col(col_name), ignorenulls=True).over(window))
                )
            
            elif isinstance(strategy, (int, float, str)):
                # Constant value
                df_cleaned = df_cleaned.fillna({col_name: strategy})
            
            logger.info(f"Applied '{strategy}' strategy to column '{col_name}'")
        
        return df_cleaned
    
    @staticmethod
    def remove_invalid_records(df: DataFrame, validation_rules: dict) -> DataFrame:
        """
        Remove records that violate validation rules.
        
        Args:
            df: Input DataFrame
            validation_rules: Dictionary mapping column names to validation functions or conditions
                            e.g., {'sale_amount': lambda x: x > 0, 'quantity': col('quantity') > 0}
            
        Returns:
            DataFrame with invalid records removed
        """
        initial_count = df.count()
        df_cleaned = df
        
        for col_name, rule in validation_rules.items():
            if col_name not in df.columns:
                continue
            
            if callable(rule):
                # Rule is a function
                df_cleaned = df_cleaned.filter(rule(col(col_name)))
            else:
                # Rule is a column expression
                df_cleaned = df_cleaned.filter(rule)
        
        final_count = df_cleaned.count()
        removed_count = initial_count - final_count
        
        logger.info(f"Removed {removed_count} invalid records based on validation rules")
        
        return df_cleaned
    
    @staticmethod
    def clean_all(df: DataFrame, config: dict = None) -> DataFrame:
        """
        Apply all cleansing operations based on configuration.
        
        Args:
            df: Input DataFrame
            config: Configuration dictionary with cleansing options
            
        Returns:
            Fully cleaned DataFrame
        """
        if config is None:
            config = {}
        
        df_cleaned = df
        
        # Remove null records
        if config.get('remove_nulls', False):
            null_columns = config.get('null_columns', None)
            df_cleaned = DataCleansing.remove_null_records(df_cleaned, null_columns)
        
        # Standardize dates
        if config.get('standardize_dates', False):
            date_columns = config.get('date_columns', {})
            df_cleaned = DataCleansing.standardize_dates(df_cleaned, date_columns)
        
        # Clean text fields
        if config.get('clean_text', False):
            text_columns = config.get('text_columns', None)
            trim_ws = config.get('trim_whitespace', True)
            uppercase = config.get('uppercase', False)
            df_cleaned = DataCleansing.clean_text_fields(df_cleaned, text_columns, trim_ws, uppercase)
        
        # Handle missing values
        if config.get('handle_missing', False):
            fill_strategy = config.get('fill_strategy', {})
            df_cleaned = DataCleansing.handle_missing_values(df_cleaned, fill_strategy)
        
        # Remove invalid records
        if config.get('remove_invalid', False):
            validation_rules = config.get('validation_rules', {})
            df_cleaned = DataCleansing.remove_invalid_records(df_cleaned, validation_rules)
        
        return df_cleaned

