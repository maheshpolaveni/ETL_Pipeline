"""
Configuration management for the ETL pipeline.
Centralized configuration for S3 paths, Delta table locations, and pipeline parameters.
"""
import os
import yaml
from typing import Dict, Any
from pathlib import Path


class Config:
    """Centralized configuration manager."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to pipeline_config.yaml. If None, uses default.
        """
        if config_path is None:
            # Default to config directory in project root
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "pipeline_config.yaml"
        
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            # Return default configuration
            return self._get_default_config()
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            's3': {
                'bucket': os.getenv('S3_BUCKET', 'retail-sales-data'),
                'raw_data_path': os.getenv('S3_RAW_PATH', 's3://retail-sales-data/raw/'),
                'processed_data_path': os.getenv('S3_PROCESSED_PATH', 's3://retail-sales-data/processed/'),
                'region': os.getenv('AWS_REGION', 'us-east-1')
            },
            'delta': {
                'database': os.getenv('DELTA_DATABASE', 'retail_sales'),
                'bronze_table': os.getenv('DELTA_BRONZE_TABLE', 'sales_bronze'),
                'silver_table': os.getenv('DELTA_SILVER_TABLE', 'sales_silver'),
                'gold_table': os.getenv('DELTA_GOLD_TABLE', 'sales_gold'),
                'table_path': os.getenv('DELTA_TABLE_PATH', '/dbfs/mnt/retail-sales/delta/')
            },
            'pipeline': {
                'incremental': os.getenv('INCREMENTAL_MODE', 'true').lower() == 'true',
                'date_column': 'sale_date',
                'partition_column': 'sale_date',
                'quality_thresholds': {
                    'null_percentage': 0.05,  # 5% max nulls
                    'min_record_count': 100,
                    'max_age_hours': 24
                }
            },
            'data_quality': {
                'critical_checks': ['record_count', 'null_percentage', 'data_freshness'],
                'warning_checks': ['business_rules', 'referential_integrity']
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 's3.bucket')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_s3_config(self) -> Dict[str, Any]:
        """Get S3 configuration."""
        return self.get('s3', {})
    
    def get_delta_config(self) -> Dict[str, Any]:
        """Get Delta Lake configuration."""
        return self.get('delta', {})
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration."""
        return self.get('pipeline', {})
    
    def get_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        return self.get('data_quality', {})


# Global configuration instance
config = Config()

