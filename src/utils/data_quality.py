"""
Data quality validation functions.
Implements checks for record counts, nulls, freshness, referential integrity, and business rules.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, max as spark_max, min as spark_min
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class DataQuality:
    """Data quality validation operations."""
    
    @staticmethod
    def check_record_count(df: DataFrame, expected_count: Optional[int] = None,
                          min_count: Optional[int] = None,
                          max_count: Optional[int] = None,
                          variance_threshold: float = 0.10) -> Dict[str, Any]:
        """
        Validate record count.
        
        Args:
            df: Input DataFrame
            expected_count: Expected number of records
            min_count: Minimum acceptable count
            max_count: Maximum acceptable count
            variance_threshold: Allowed variance percentage if expected_count is provided
            
        Returns:
            Dictionary with validation results
        """
        actual_count = df.count()
        
        result = {
            'check_name': 'record_count',
            'status': 'PASS',
            'actual_count': actual_count,
            'message': ''
        }
        
        if expected_count is not None:
            variance = abs(actual_count - expected_count) / expected_count if expected_count > 0 else 0
            result['expected_count'] = expected_count
            result['variance'] = variance
            
            if variance > variance_threshold:
                result['status'] = 'FAIL'
                result['message'] = f"Record count variance {variance:.2%} exceeds threshold {variance_threshold:.2%}"
            else:
                result['message'] = f"Record count within acceptable variance: {variance:.2%}"
        
        if min_count is not None and actual_count < min_count:
            result['status'] = 'FAIL'
            result['message'] = f"Record count {actual_count} is below minimum {min_count}"
        
        if max_count is not None and actual_count > max_count:
            result['status'] = 'FAIL'
            result['message'] = f"Record count {actual_count} exceeds maximum {max_count}"
        
        logger.info(f"Record count check: {result['status']} - {result['message']}")
        
        return result
    
    @staticmethod
    def check_null_percentage(df: DataFrame, columns: Optional[List[str]] = None,
                             threshold: float = 0.05) -> Dict[str, Any]:
        """
        Check null percentage in specified columns.
        
        Args:
            df: Input DataFrame
            columns: List of columns to check. If None, checks all columns.
            threshold: Maximum acceptable null percentage (0.05 = 5%)
            
        Returns:
            Dictionary with validation results
        """
        if columns is None:
            columns = df.columns
        
        total_records = df.count()
        if total_records == 0:
            return {
                'check_name': 'null_percentage',
                'status': 'WARNING',
                'message': 'No records to check',
                'null_percentages': {}
            }
        
        null_percentages = {}
        failed_columns = []
        
        for col_name in columns:
            if col_name not in df.columns:
                continue
            
            null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
            null_pct = null_count / total_records if total_records > 0 else 0
            null_percentages[col_name] = null_pct
            
            if null_pct > threshold:
                failed_columns.append(f"{col_name}: {null_pct:.2%}")
        
        status = 'FAIL' if failed_columns else 'PASS'
        message = f"Columns exceeding threshold: {', '.join(failed_columns)}" if failed_columns else "All columns within null threshold"
        
        result = {
            'check_name': 'null_percentage',
            'status': status,
            'threshold': threshold,
            'null_percentages': null_percentages,
            'failed_columns': failed_columns,
            'message': message
        }
        
        logger.info(f"Null percentage check: {status} - {message}")
        
        return result
    
    @staticmethod
    def check_data_freshness(df: DataFrame, timestamp_column: str,
                            max_age_hours: int = 24) -> Dict[str, Any]:
        """
        Check data freshness based on maximum timestamp.
        
        Args:
            df: Input DataFrame
            timestamp_column: Name of timestamp column
            max_age_hours: Maximum acceptable age in hours
            
        Returns:
            Dictionary with validation results
        """
        if timestamp_column not in df.columns:
            return {
                'check_name': 'data_freshness',
                'status': 'ERROR',
                'message': f"Timestamp column '{timestamp_column}' not found"
            }
        
        try:
            max_timestamp = df.agg(spark_max(col(timestamp_column))).collect()[0][0]
            
            if max_timestamp is None:
                return {
                    'check_name': 'data_freshness',
                    'status': 'WARNING',
                    'message': 'No timestamp values found'
                }
            
            # Convert to datetime if needed
            if isinstance(max_timestamp, str):
                max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00'))
            
            current_time = datetime.now()
            age_hours = (current_time - max_timestamp).total_seconds() / 3600
            
            status = 'FAIL' if age_hours > max_age_hours else 'PASS'
            message = f"Data is {age_hours:.1f} hours old (max: {max_age_hours} hours)"
            
            result = {
                'check_name': 'data_freshness',
                'status': status,
                'max_timestamp': str(max_timestamp),
                'age_hours': age_hours,
                'max_age_hours': max_age_hours,
                'message': message
            }
            
            logger.info(f"Data freshness check: {status} - {message}")
            
            return result
        
        except Exception as e:
            return {
                'check_name': 'data_freshness',
                'status': 'ERROR',
                'message': f"Error checking data freshness: {str(e)}"
            }
    
    @staticmethod
    def check_business_rules(df: DataFrame, rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate business rules.
        
        Args:
            df: Input DataFrame
            rules: Dictionary of business rules
                  e.g., {'sale_amount': {'min': 0, 'max': 100000}, 'quantity': {'min': 1}}
            
        Returns:
            Dictionary with validation results
        """
        failed_rules = []
        rule_details = {}
        
        for column, rule_config in rules.items():
            if column not in df.columns:
                continue
            
            rule_detail = {'column': column, 'violations': 0}
            
            # Check min value
            if 'min' in rule_config:
                min_val = rule_config['min']
                violations = df.filter(col(column) < min_val).count()
                if violations > 0:
                    failed_rules.append(f"{column} < {min_val}: {violations} violations")
                    rule_detail['violations'] += violations
                    rule_detail['min_violations'] = violations
            
            # Check max value
            if 'max' in rule_config:
                max_val = rule_config['max']
                violations = df.filter(col(column) > max_val).count()
                if violations > 0:
                    failed_rules.append(f"{column} > {max_val}: {violations} violations")
                    rule_detail['violations'] += violations
                    rule_detail['max_violations'] = violations
            
            # Check allowed values
            if 'allowed_values' in rule_config:
                allowed = rule_config['allowed_values']
                violations = df.filter(~col(column).isin(allowed)).count()
                if violations > 0:
                    failed_rules.append(f"{column} not in {allowed}: {violations} violations")
                    rule_detail['violations'] += violations
                    rule_detail['allowed_values_violations'] = violations
            
            rule_details[column] = rule_detail
        
        status = 'FAIL' if failed_rules else 'PASS'
        message = f"Failed rules: {', '.join(failed_rules)}" if failed_rules else "All business rules passed"
        
        result = {
            'check_name': 'business_rules',
            'status': status,
            'failed_rules': failed_rules,
            'rule_details': rule_details,
            'message': message
        }
        
        logger.info(f"Business rules check: {status} - {message}")
        
        return result
    
    @staticmethod
    def check_referential_integrity(df: DataFrame, 
                                   reference_df: DataFrame,
                                   key_columns: List[str]) -> Dict[str, Any]:
        """
        Check referential integrity between two DataFrames.
        
        Args:
            df: Input DataFrame (fact table)
            reference_df: Reference DataFrame (dimension table)
            key_columns: Columns to join on
            
        Returns:
            Dictionary with validation results
        """
        # Get distinct keys from reference
        valid_keys = reference_df.select(key_columns).distinct()
        
        # Check for orphaned records
        orphaned = df.join(valid_keys, on=key_columns, how='left_anti')
        orphaned_count = orphaned.count()
        
        status = 'FAIL' if orphaned_count > 0 else 'PASS'
        message = f"Found {orphaned_count} records with invalid foreign keys" if orphaned_count > 0 else "Referential integrity maintained"
        
        result = {
            'check_name': 'referential_integrity',
            'status': status,
            'orphaned_count': orphaned_count,
            'message': message
        }
        
        logger.info(f"Referential integrity check: {status} - {message}")
        
        return result
    
    @staticmethod
    def run_all_checks(df: DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run all configured data quality checks.
        
        Args:
            df: Input DataFrame
            config: Configuration dictionary with check settings
            
        Returns:
            Dictionary with all check results
        """
        results = {}
        
        # Record count check
        if config.get('check_record_count', True):
            results['record_count'] = DataQuality.check_record_count(
                df,
                expected_count=config.get('expected_count'),
                min_count=config.get('min_count'),
                variance_threshold=config.get('variance_threshold', 0.10)
            )
        
        # Null percentage check
        if config.get('check_nulls', True):
            results['null_percentage'] = DataQuality.check_null_percentage(
                df,
                columns=config.get('null_check_columns'),
                threshold=config.get('null_threshold', 0.05)
            )
        
        # Data freshness check
        if config.get('check_freshness', True):
            timestamp_col = config.get('timestamp_column', 'sale_date')
            max_age = config.get('max_age_hours', 24)
            results['data_freshness'] = DataQuality.check_data_freshness(
                df, timestamp_col, max_age
            )
        
        # Business rules check
        if config.get('check_business_rules', True):
            business_rules = config.get('business_rules', {})
            if business_rules:
                results['business_rules'] = DataQuality.check_business_rules(df, business_rules)
        
        # Referential integrity check
        if config.get('check_referential_integrity', False):
            ref_df = config.get('reference_df')
            key_cols = config.get('referential_key_columns', [])
            if ref_df and key_cols:
                results['referential_integrity'] = DataQuality.check_referential_integrity(
                    df, ref_df, key_cols
                )
        
        # Overall status
        critical_checks = config.get('critical_checks', [])
        has_failures = any(
            results[check]['status'] == 'FAIL'
            for check in results.keys()
            if check in critical_checks
        )
        
        results['overall_status'] = 'FAIL' if has_failures else 'PASS'
        results['summary'] = {
            'total_checks': len(results) - 1,  # Exclude overall_status
            'passed': sum(1 for r in results.values() if isinstance(r, dict) and r.get('status') == 'PASS'),
            'failed': sum(1 for r in results.values() if isinstance(r, dict) and r.get('status') == 'FAIL'),
            'warnings': sum(1 for r in results.values() if isinstance(r, dict) and r.get('status') == 'WARNING')
        }
        
        logger.info(f"Data quality checks completed. Overall status: {results['overall_status']}")
        
        return results

