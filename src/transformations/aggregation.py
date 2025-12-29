"""
Data aggregation transformations.
Creates daily/weekly/monthly sales summaries, product-level and customer-level aggregations.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    date_format, year, month, weekofyear, dayofmonth,
    round as spark_round
)
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class Aggregation:
    """Data aggregation operations."""
    
    @staticmethod
    def daily_sales_summary(df: DataFrame, 
                           date_column: str = 'sale_date',
                           amount_column: str = 'sale_amount',
                           quantity_column: str = 'quantity') -> DataFrame:
        """
        Create daily sales summary.
        
        Args:
            df: Input DataFrame
            date_column: Name of date column
            amount_column: Name of sales amount column
            quantity_column: Name of quantity column
            
        Returns:
            DataFrame with daily aggregations
        """
        daily_summary = df.groupBy(date_column).agg(
            spark_sum(amount_column).alias('total_revenue'),
            spark_sum(quantity_column).alias('total_quantity'),
            count('*').alias('transaction_count'),
            avg(amount_column).alias('avg_transaction_value'),
            spark_max(amount_column).alias('max_transaction_value'),
            spark_min(amount_column).alias('min_transaction_value')
        ).orderBy(date_column)
        
        logger.info("Created daily sales summary")
        
        return daily_summary
    
    @staticmethod
    def weekly_sales_summary(df: DataFrame,
                            date_column: str = 'sale_date',
                            amount_column: str = 'sale_amount',
                            quantity_column: str = 'quantity') -> DataFrame:
        """
        Create weekly sales summary.
        
        Args:
            df: Input DataFrame
            date_column: Name of date column
            amount_column: Name of sales amount column
            quantity_column: Name of quantity column
            
        Returns:
            DataFrame with weekly aggregations
        """
        df_with_week = df.withColumn('year', year(col(date_column))) \
                        .withColumn('week', weekofyear(col(date_column)))
        
        weekly_summary = df_with_week.groupBy('year', 'week').agg(
            spark_sum(amount_column).alias('total_revenue'),
            spark_sum(quantity_column).alias('total_quantity'),
            count('*').alias('transaction_count'),
            avg(amount_column).alias('avg_transaction_value')
        ).orderBy('year', 'week')
        
        logger.info("Created weekly sales summary")
        
        return weekly_summary
    
    @staticmethod
    def monthly_sales_summary(df: DataFrame,
                             date_column: str = 'sale_date',
                             amount_column: str = 'sale_amount',
                             quantity_column: str = 'quantity') -> DataFrame:
        """
        Create monthly sales summary.
        
        Args:
            df: Input DataFrame
            date_column: Name of date column
            amount_column: Name of sales amount column
            quantity_column: Name of quantity column
            
        Returns:
            DataFrame with monthly aggregations
        """
        df_with_month = df.withColumn('year', year(col(date_column))) \
                         .withColumn('month', month(col(date_column)))
        
        monthly_summary = df_with_month.groupBy('year', 'month').agg(
            spark_sum(amount_column).alias('total_revenue'),
            spark_sum(quantity_column).alias('total_quantity'),
            count('*').alias('transaction_count'),
            avg(amount_column).alias('avg_transaction_value')
        ).orderBy('year', 'month')
        
        logger.info("Created monthly sales summary")
        
        return monthly_summary
    
    @staticmethod
    def product_level_aggregation(df: DataFrame,
                                  product_id_column: str = 'product_id',
                                  amount_column: str = 'sale_amount',
                                  quantity_column: str = 'quantity',
                                  additional_grouping: Optional[List[str]] = None) -> DataFrame:
        """
        Create product-level aggregations.
        
        Args:
            df: Input DataFrame
            product_id_column: Name of product ID column
            amount_column: Name of sales amount column
            quantity_column: Name of quantity column
            additional_grouping: Optional additional columns to group by (e.g., ['region', 'store_id'])
            
        Returns:
            DataFrame with product-level aggregations
        """
        group_by_cols = [product_id_column]
        if additional_grouping:
            group_by_cols.extend(additional_grouping)
        
        product_summary = df.groupBy(group_by_cols).agg(
            spark_sum(amount_column).alias('total_revenue'),
            spark_sum(quantity_column).alias('total_quantity_sold'),
            count('*').alias('transaction_count'),
            avg(amount_column).alias('avg_sale_price'),
            spark_max(amount_column).alias('max_sale_price'),
            spark_min(amount_column).alias('min_sale_price')
        ).orderBy(spark_sum(amount_column).desc())
        
        logger.info(f"Created product-level aggregation grouped by: {group_by_cols}")
        
        return product_summary
    
    @staticmethod
    def customer_level_aggregation(df: DataFrame,
                                   customer_id_column: str = 'customer_id',
                                   amount_column: str = 'sale_amount',
                                   quantity_column: str = 'quantity',
                                   date_column: str = 'sale_date') -> DataFrame:
        """
        Create customer-level aggregations.
        
        Args:
            df: Input DataFrame
            customer_id_column: Name of customer ID column
            amount_column: Name of sales amount column
            quantity_column: Name of quantity column
            date_column: Name of date column
            
        Returns:
            DataFrame with customer-level aggregations
        """
        customer_summary = df.groupBy(customer_id_column).agg(
            spark_sum(amount_column).alias('total_revenue'),
            spark_sum(quantity_column).alias('total_quantity_purchased'),
            count('*').alias('transaction_count'),
            avg(amount_column).alias('avg_transaction_value'),
            spark_max(date_column).alias('last_purchase_date'),
            spark_min(date_column).alias('first_purchase_date')
        ).orderBy(spark_sum(amount_column).desc())
        
        logger.info("Created customer-level aggregation")
        
        return customer_summary
    
    @staticmethod
    def revenue_calculations(df: DataFrame,
                            amount_column: str = 'sale_amount',
                            quantity_column: str = 'quantity',
                            discount_column: Optional[str] = None) -> DataFrame:
        """
        Calculate revenue metrics.
        
        Args:
            df: Input DataFrame
            amount_column: Name of sales amount column
            quantity_column: Name of quantity column
            discount_column: Optional discount column name
            
        Returns:
            DataFrame with revenue calculations added
        """
        df_with_revenue = df
        
        # Calculate total revenue per transaction
        if discount_column and discount_column in df.columns:
            df_with_revenue = df_with_revenue.withColumn(
                'net_revenue',
                col(amount_column) * col(quantity_column) - col(discount_column)
            )
        else:
            df_with_revenue = df_with_revenue.withColumn(
                'net_revenue',
                col(amount_column) * col(quantity_column)
            )
        
        # Calculate unit price
        df_with_revenue = df_with_revenue.withColumn(
            'unit_price',
            spark_round(col(amount_column) / col(quantity_column), 2)
        )
        
        logger.info("Added revenue calculations")
        
        return df_with_revenue
    
    @staticmethod
    def create_all_aggregations(df: DataFrame, config: dict = None) -> dict:
        """
        Create all standard aggregations based on configuration.
        
        Args:
            df: Input DataFrame
            config: Configuration dictionary with aggregation options
            
        Returns:
            Dictionary of aggregated DataFrames
        """
        if config is None:
            config = {}
        
        aggregations = {}
        
        date_col = config.get('date_column', 'sale_date')
        amount_col = config.get('amount_column', 'sale_amount')
        quantity_col = config.get('quantity_column', 'quantity')
        
        # Daily summary
        if config.get('daily_summary', True):
            aggregations['daily'] = Aggregation.daily_sales_summary(
                df, date_col, amount_col, quantity_col
            )
        
        # Weekly summary
        if config.get('weekly_summary', False):
            aggregations['weekly'] = Aggregation.weekly_sales_summary(
                df, date_col, amount_col, quantity_col
            )
        
        # Monthly summary
        if config.get('monthly_summary', True):
            aggregations['monthly'] = Aggregation.monthly_sales_summary(
                df, date_col, amount_col, quantity_col
            )
        
        # Product-level aggregation
        if config.get('product_aggregation', True):
            product_col = config.get('product_id_column', 'product_id')
            additional_grouping = config.get('product_grouping', None)
            aggregations['product'] = Aggregation.product_level_aggregation(
                df, product_col, amount_col, quantity_col, additional_grouping
            )
        
        # Customer-level aggregation
        if config.get('customer_aggregation', True):
            customer_col = config.get('customer_id_column', 'customer_id')
            aggregations['customer'] = Aggregation.customer_level_aggregation(
                df, customer_col, amount_col, quantity_col, date_col
            )
        
        logger.info(f"Created {len(aggregations)} aggregation types")
        
        return aggregations

