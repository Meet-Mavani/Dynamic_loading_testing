from typing import Optional, Literal, List, Dict, Any
from datetime import datetime
from strands import Agent, tool
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import Error
import psycopg2.pool

# Database configuration - update with your actual PostgreSQL details
DB_CONFIG = {
    "host": "secai-database.cluster-c4timsc6k2gq.us-east-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "lgUKL_:1X|AIjqAC-jXuXW-6V7G9",
    "database": "testing_db",
    "port": 5432
}

TABLE_NAME = "bing_advertising_data"  # Update with your actual table name

# Global connection pool
connection_pool = None

# -------------------------------------------------------------------
# Connection Pool Setup
# -------------------------------------------------------------------

def initialize_connection_pool():
    """Initialize connection pool"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,  # min and max connections
            **DB_CONFIG
        )
        return connection_pool
    except Error as e:
        raise Exception(f"Connection pool initialization failed: {e}")

def get_db_connection():
    """Get database connection from pool"""
    global connection_pool
    if connection_pool is None:
        try:
            initialize_connection_pool()
        except Exception as e:
            print(f"Failed to initialize connection pool: {e}")
            raise

    try:
        connection = connection_pool.getconn()
        if connection is None:
            raise Exception("Failed to get connection from pool")
        return connection
    except Error as e:
        raise Exception(f"Database connection failed: {e}")

def return_connection(connection):
    """Return connection to pool"""
    global connection_pool
    if connection_pool and connection:
        connection_pool.putconn(connection)

# -------------------------------------------------------------------
# Tools
# -------------------------------------------------------------------
@tool(name="bing_agent_tools",description="Query and analyze Bing Ads campaign performance data")
def bing_agent_tools(prompt: str) -> str:
    """Main Bing Ads agent tool that routes to appropriate sub-tools"""
    try:
        print(f"Bing agent received prompt: {prompt}")
        supervisor_agent = Agent(
            tools=[get_campaign_performance_for_bing, get_campaign_trends, search_similar_campaigns]
        )
        
        response = supervisor_agent(prompt)
        print(f"Bing agent response: {response}")
        return str(response)  # Ensure string return

    except Exception as e:
        error_msg = f"Error in bing_agent_tools: {str(e)}"
        print(error_msg)
        return error_msg

    

@tool(name="get_campaign_performance_for_bing", 
      description="Get campaign performance metrics including impressions, clicks, spend, conversions for specific campaigns or time periods. Use this when users ask about campaign performance, ROI, CTR, or conversion rates.")
def get_campaign_performance_for_bing(
    campaign_name: Optional[str] = None,
    campaign_id: Optional[str] = None,
    source: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    time_operator: Literal['>', '<', '>=', '<=', '=', 'between'] = 'between',
    metrics: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> str:
    """
    Get campaign performance data with flexible filtering
    
    Args:
        campaign_name: Filter by specific campaign name (partial match supported)
        campaign_id: Filter by specific campaign ID
        date_from: Start date (YYYY-MM-DD format)
        source: Filter by traffic source
        date_to: End date (YYYY-MM-DD format) 
        time_operator: How to apply date filtering
        metrics: List of metrics to return ['impressions', 'clicks', 'spend', 'conversions']
        limit: Return top N campaigns by spend
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Build SELECT clause
        if metrics:
            metric_columns = ', '.join(metrics)
            select_clause = f"date, campaign_id, campaign_name, {metric_columns}"
        else:
            select_clause = "date, campaign_id, campaign_name, impressions, clicks, spend, conversions"
        
        # Build base query with calculated metrics
        query = f"""
        SELECT {select_clause},
               ROUND((clicks::DECIMAL / NULLIF(impressions, 0)) * 100, 2) as ctr,
               ROUND(spend::DECIMAL / NULLIF(clicks, 0), 2) as cpc,
               ROUND((conversions::DECIMAL / NULLIF(clicks, 0)) * 100, 2) as conversion_rate,
               ROUND(spend::DECIMAL / NULLIF(conversions, 0), 2) as cost_per_conversion
        FROM {TABLE_NAME} 
        """
        
        # Build WHERE conditions
        where_conditions = ["source = 'Bing Ads'"]
        params = []
        
        if campaign_name:
            where_conditions.append("LOWER(campaign_name) LIKE LOWER(%s)")
            params.append(f"%{campaign_name}%")
        
        if campaign_id:
            where_conditions.append("campaign_id = %s")
            params.append(campaign_id)
        
        # Date filtering
        if date_from and date_to and time_operator == 'between':
            where_conditions.append("date BETWEEN %s AND %s")
            params.extend([date_from, date_to])
        elif date_from:
            where_conditions.append(f"date {time_operator} %s")
            params.append(date_from)
        
        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        
        # Add ORDER BY and LIMIT
        if limit:
            query += " ORDER BY spend DESC LIMIT %s"
            params.append(limit)
        else:
            query += " ORDER BY date DESC"
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        cursor.execute(query, params)
        campaign_results = cursor.fetchall()
        
        # Get summary statistics
        summary_query = f"""
        SELECT 
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            ROUND(SUM(spend), 2) as total_spend,
            SUM(conversions) as total_conversions,
            ROUND(AVG((clicks::DECIMAL / NULLIF(impressions, 0)) * 100), 2) as average_ctr,
            ROUND(AVG(spend::DECIMAL / NULLIF(clicks, 0)), 2) as average_cpc,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(clicks), 0)) * 100, 2) as overall_conversion_rate,
            COUNT(DISTINCT campaign_id) as campaign_count,
            MIN(date) as start_date,
            MAX(date) as end_date
        FROM {TABLE_NAME}
        """
        
        if where_conditions:
            summary_query += " WHERE " + " AND ".join(where_conditions)
        
        cursor.execute(summary_query, params[:-1] if limit and params else params)
        summary_result = cursor.fetchone()
        
        # Format output
        output = "Campaign Performance Report\n" + "=" * 50 + "\n\n"
        
        if summary_result:
            output += (
                f"Summary Statistics:\n"
                f"Total Campaigns: {summary_result['campaign_count']:,}\n"
                f"Date Range: {summary_result['start_date']} to {summary_result['end_date']}\n"
                f"Total Impressions: {summary_result['total_impressions']:,}\n"
                f"Total Clicks: {summary_result['total_clicks']:,}\n"
                f"Total Spend: ${summary_result['total_spend']:,.2f}\n"
                f"Total Conversions: {summary_result['total_conversions']:,}\n"
                f"Average CTR: {summary_result['average_ctr'] or 0}%\n"
                f"Average CPC: ${summary_result['average_cpc'] or 0:.2f}\n"
                f"Overall Conversion Rate: {summary_result['overall_conversion_rate'] or 0}%\n\n"
            )
        
        if campaign_results:
            output += f"Campaign Details ({len(campaign_results)} campaigns):\n" + "-" * 50 + "\n"
            for row in campaign_results:
                output += (
                    f"Date: {row['date']}\n"
                    f"Campaign: {row['campaign_name']} (ID: {row['campaign_id']})\n"
                    f"Impressions: {row['impressions']:,} | Clicks: {row['clicks']:,}\n"
                    f"Spend: ${row['spend']:.2f} | Conversions: {row['conversions']}\n"
                    f"CTR: {row['ctr'] or 0}% | CPC: ${row['cpc'] or 0:.2f}\n"
                    f"Conversion Rate: {row['conversion_rate'] or 0}%\n"
                    + "-" * 50 + "\n"
                )
        else:
            output += "No campaign data found matching the criteria.\n"
        
        return output
        
    except Exception as e:
        return f"Error processing request: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)

@tool(name="get_campaign_trends", 
      description="Analyze campaign performance trends over time. Use this when users ask about performance trends, seasonal patterns, or time-based analysis.")
def get_campaign_trends(
    campaign_name: Optional[str] = None,
    period: Literal['daily', 'weekly', 'monthly'] = 'daily',
    metric: Literal['impressions', 'clicks', 'spend', 'conversions', 'ctr', 'cpc'] = 'spend',
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    days_back: Optional[int] = 30
) -> str:
    """
    Analyze trends in campaign performance over time
    
    Args:
        campaign_name: Filter by specific campaign name
        period: Aggregation period (daily, weekly, monthly)
        metric: Metric to analyze trends for
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        days_back: Number of days to look back if no date range specified
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Build WHERE conditions
        where_conditions = ["source = 'Bing Ads'"]
        params = []
        
        if campaign_name:
            where_conditions.append("LOWER(campaign_name) LIKE LOWER(%s)")
            params.append(f"%{campaign_name}%")
        
        if date_from:
            where_conditions.append("date >= %s")
            params.append(date_from)
        elif not date_to:  # If no date range specified, use days_back
            where_conditions.append(f"date >= CURRENT_DATE - INTERVAL '{days_back} DAYS'")
        
        if date_to:
            where_conditions.append("date <= %s")
            params.append(date_to)
        
        where_clause = " AND ".join(where_conditions)
        
        # Build period grouping
        if period == 'weekly':
            period_group = "DATE_TRUNC('week', date)"
            period_format = "TO_CHAR(DATE_TRUNC('week', date), 'YYYY-MM-DD')"
        elif period == 'monthly':
            period_group = "DATE_TRUNC('month', date)"
            period_format = "TO_CHAR(DATE_TRUNC('month', date), 'YYYY-MM')"
        else:  # daily
            period_group = "date"
            period_format = "TO_CHAR(date, 'YYYY-MM-DD')"
        
        query = f"""
        SELECT 
            {period_format} as period,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            ROUND(SUM(spend), 2) as spend,
            SUM(conversions) as conversions,
            ROUND((SUM(clicks)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 2) as ctr,
            ROUND(SUM(spend)::DECIMAL / NULLIF(SUM(clicks), 0), 2) as cpc
        FROM {TABLE_NAME} 
        WHERE {where_clause}
        GROUP BY {period_group}
        ORDER BY {period_group}
        """
        
        print(f"Executing trends query: {query}")
        print(f"Parameters: {params}")
        
        cursor.execute(query, params)
        trend_results = cursor.fetchall()
        
        if not trend_results:
            return f"No trend data found for the specified criteria."
        
        output = f"Campaign Trends Analysis ({period.title()})\n" + "=" * 50 + "\n\n"
        
        if len(trend_results) > 1:
            first_value = float(trend_results[0].get(metric, 0))
            last_value = float(trend_results[-1].get(metric, 0))
            
            if first_value > 0:
                percent_change = ((last_value - first_value) / first_value) * 100
                trend_direction = "increasing" if last_value > first_value else "decreasing"
                output += f"Trend Direction: {trend_direction.title()} ({percent_change:+.1f}%)\n"
                output += f"Periods Analyzed: {len(trend_results)}\n\n"
        
        # Display trend data
        for row in trend_results:
            output += (
                f"{row['period']}: "
                f"Impressions: {row['impressions']:,} | "
                f"Clicks: {row['clicks']:,} | "
                f"Spend: ${row['spend']:,.2f} | "
                f"Conversions: {row['conversions']} | "
                f"CTR: {row['ctr'] or 0}% | "
                f"CPC: ${row['cpc'] or 0:.2f}\n"
            )
        
        return output
        
    except Exception as e:
        return f"Error processing trends: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)



@tool(name="search_similar_campaigns", 
      description="Search for campaigns by name or get campaigns from specific time periods. Use this when users want to find specific campaigns or explore what campaigns were running.")
def search_similar_campaigns(
    search_term: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    min_spend: Optional[float] = None,
    has_conversions: Optional[bool] = None,
    limit: Optional[int] = 20
) -> str:
    """
    Search and filter campaigns based on various criteria
    
    Args:
        search_term: Search term to find in campaign names
        date_from: Start date filter (YYYY-MM-DD)
        date_to: End date filter (YYYY-MM-DD)
        min_spend: Minimum total spend filter
        has_conversions: Filter campaigns that have/don't have conversions
        limit: Maximum number of results to return
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Build WHERE conditions
        where_conditions = ["source = 'Bing Ads'"]
        params = []
        
        if search_term:
            where_conditions.append("LOWER(campaign_name) LIKE LOWER(%s)")
            params.append(f"%{search_term}%")
        
        if date_from:
            where_conditions.append("date >= %s")
            params.append(date_from)
        if date_to:
            where_conditions.append("date <= %s")
            params.append(date_to)
        
        where_clause = " AND ".join(where_conditions)
        
        # Build HAVING conditions for aggregated filters
        having_conditions = []
        if min_spend:
            having_conditions.append("SUM(spend) >= %s")
            params.append(min_spend)
        
        if has_conversions is not None:
            if has_conversions:
                having_conditions.append("SUM(conversions) > 0")
            else:
                having_conditions.append("SUM(conversions) = 0")
        
        having_clause = ""
        if having_conditions:
            having_clause = f"HAVING {' AND '.join(having_conditions)}"
        
        query = f"""
        SELECT 
            campaign_id,
            campaign_name,
            ROUND(SUM(spend), 2) as total_spend,
            SUM(conversions) as total_conversions,
            SUM(clicks) as total_clicks,
            SUM(impressions) as total_impressions,
            MIN(date) as start_date,
            MAX(date) as end_date,
            COUNT(DISTINCT date) as days_active,
            ROUND((SUM(clicks)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 2) as ctr,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(clicks), 0)) * 100, 2) as conversion_rate
        FROM {TABLE_NAME}
        WHERE {where_clause}
        GROUP BY campaign_id, campaign_name
        {having_clause}
        ORDER BY total_spend DESC
        LIMIT %s
        """
        
        params.append(limit)
        
        print(f"Executing search campaigns query: {query}")
        print(f"Parameters: {params}")
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        if not results:
            return f"No campaigns found matching the search criteria."
        
        # Format output
        output = f"Campaign Search Results\n" + "=" * 40 + "\n"
        output += f"Search Criteria: {search_term or 'All campaigns'}\n"
        if date_from or date_to:
            output += f"Date Range: {date_from or 'Start'} to {date_to or 'End'}\n"
        if min_spend:
            output += f"Minimum Spend: ${min_spend:,.2f}\n"
        if has_conversions is not None:
            output += f"Has Conversions: {'Yes' if has_conversions else 'No'}\n"
        output += f"Found {len(results)} campaigns\n\n"
        
        for row in results:
            output += (
                f"Campaign: {row['campaign_name']}\n"
                f"ID: {row['campaign_id']}\n"
                f"Period: {row['start_date']} to {row['end_date']} ({row['days_active']} days)\n"
                f"Total Spend: ${row['total_spend']:,.2f}\n"
                f"Impressions: {row['total_impressions']:,} | Clicks: {row['total_clicks']:,}\n"
                f"Conversions: {row['total_conversions']} | CTR: {row['ctr'] or 0}%\n"
                f"Conversion Rate: {row['conversion_rate'] or 0}%\n"
                + "-" * 50 + "\n"
            )
        
        return output
        
    except Exception as e:
        return f"Error searching campaigns: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)

def cleanup_connections():
    """Clean up connection pool"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()

