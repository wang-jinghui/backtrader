"""
Module for reading fund NAV data from MySQL database.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import List, Optional
import pathlib


# 获取当前文件所在目录的父目录路径，确保能正确找到 .env 文件
current_dir = pathlib.Path(__file__).parent
env_path = current_dir / '.env'

# 检查 .env 文件是否存在
if env_path.exists():
    # 加载当前目录下的 .env 文件
    load_dotenv(env_path)
else:
    # 尝试在项目根目录查找 .env 文件
    project_root = current_dir.parent
    env_path = project_root / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    else:
        print(f"Warning: .env file not found at {current_dir} or {project_root}")

class FundNAVReader:
    """Class to handle fund NAV data queries."""
    
    def __init__(self):
        """Initialize the database connection."""
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not found")
        
        self.engine = create_engine(self.database_url)
    
    def get_fund_nav_data(
        self, 
        fund_codes: Optional[List[str]] = None, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query fund NAV data with optional filters.
        
        Args:
            fund_codes: List of fund codes to filter by (optional)
            start_date: Start date in format 'YYYY-MM-DD' (optional)
            end_date: End date in format 'YYYY-MM-DD' (optional)
            
        Returns:
            pandas.DataFrame: Query results containing fund NAV data
        """
        # Base query
        query = "SELECT * FROM fund_nav"
        conditions = []
        params = {}
        
        # Add conditions based on provided parameters
        if fund_codes:
            placeholders = ','.join([f':fund_code_{i}' for i in range(len(fund_codes))])
            conditions.append(f"fund_code IN ({placeholders})")
            for i, code in enumerate(fund_codes):
                params[f'fund_code_{i}'] = code
        
        if start_date:
            conditions.append("nav_date >= :start_date")
            params['start_date'] = start_date
            
        if end_date:
            conditions.append("nav_date <= :end_date")
            params['end_date'] = end_date
        
        # Add WHERE clause if there are any conditions
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Execute query using pandas
        with self.engine.connect() as connection:
            # 解决pandas.read_sql不接受SQLAlchemy text对象的问题
            result = connection.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        return df


# Global instance for convenience
_fund_nav_reader = FundNAVReader()


def get_fund_nav_data(
    fund_codes: Optional[List[str]] = None, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Query fund NAV data with optional filters.
    
    Args:
        fund_codes: List of fund codes to filter by (optional)
        start_date: Start date in format 'YYYY-MM-DD' (optional)
        end_date: End date in format 'YYYY-MM-DD' (optional)
        
    Returns:
        pandas.DataFrame: Query results containing fund NAV data
    """
    return _fund_nav_reader.get_fund_nav_data(fund_codes, start_date, end_date)


def get_all_fund_codes() -> List[str]:
    """
    Get all unique fund codes from the database.
    
    Returns:
        List of unique fund codes
    """
    with _fund_nav_reader.engine.connect() as connection:
        result = connection.execute(text("SELECT DISTINCT fund_code FROM fund_nav ORDER BY fund_code"))
        fund_codes = [row[0] for row in result]
    
    return fund_codes


def get_date_range() -> tuple:
    """
    Get the min and max date range available in the fund_nav table.
    
    Returns:
        Tuple of (min_date, max_date)
    """
    with _fund_nav_reader.engine.connect() as connection:
        result = connection.execute(text("SELECT MIN(nav_date), MAX(nav_date) FROM fund_nav"))
        min_date, max_date = result.fetchone()
    
    return min_date, max_date


def test_connection():
    """
    Test database connection and print status.
    """
    try:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            print("ERROR: DATABASE_URL environment variable not found")
            return False
        
        print(f"Database URL found: {'Yes' if database_url else 'No'}")
        print(f"URL preview: {database_url[:50]}..." if len(database_url) > 50 else f"Full URL: {database_url}")
        
        # Try to establish connection and run simple query
        with _fund_nav_reader.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fund_nav LIMIT 1"))
            count = result.scalar()
            print(f"Connection successful! Found {count} records in fund_nav table")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    print("Example queries:")
    
    # Get all data
    all_data = get_fund_nav_data()
    print(f"All data shape: {all_data.shape}")
    
    # Get specific funds
    fund_codes_example = ["FUND001", "FUND002"]
    specific_funds = get_fund_nav_data(fund_codes=fund_codes_example)
    print(f"Specific funds data shape: {specific_funds.shape}")
    
    # Get data within date range
    date_range_data = get_fund_nav_data(start_date="2023-01-01", end_date="2023-12-31")
    print(f"Date range data shape: {date_range_data.shape}")
    
    # Get specific funds within date range
    filtered_data = get_fund_nav_data(
        fund_codes=["FUND001", "FUND002"], 
        start_date="2023-01-01", 
        end_date="2023-12-31"
    )
    print(f"Filtered data shape: {filtered_data.shape}")
    
    # Get available fund codes
    available_funds = get_all_fund_codes()
    print(f"Available fund codes (first 5): {available_funds[:5]}")
    
    # Get date range
    min_date, max_date = get_date_range()
    print(f"Data date range: {min_date} to {max_date}")