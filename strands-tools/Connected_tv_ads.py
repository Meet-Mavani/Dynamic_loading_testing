from datetime import datetime
from strands import Agent, tool
from typing import Optional,Literal,List
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import Error
import psycopg2.pool

# Database configuration - update with your actual PostgreSQL details
DB_CONFIG = {
    "host": "secai-database.cluster-c4timsc6k2gq.us-east-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "h9hS5I9eaB5Vyot2kU03w|3P?nC_",
    "database": "testing_db",
    "port": 5432
}

TABLE_NAME = "connected_tv_ads"

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
        initialize_connection_pool()

    try:
        connection = connection_pool.getconn()
        return connection
    except Error as e:
        raise Exception(f"Database connection failed: {e}")

def return_connection(connection):
    """Return connection to pool"""
    global connection_pool
    if connection_pool and connection:
        connection_pool.putconn(connection)

# -------------------------------------------------------------------
# Main Agent Function
# -------------------------------------------------------------------
@tool(name="connected_tv_agent",description="This tool is used to give responses related to Connected tv ads prompts")

def connected_tv_agent(prompt: str) -> str:
    """Main Connected TV agent tool that routes to appropriate sub-tools"""
    supervisor_agent = Agent(
        tools=[search_similar_ad_slot,get_platform_trends,get_platform_performance]
    )
    
    response = supervisor_agent(prompt)
    

@tool(
    name="get_platform_performance", 
    description="""
    Get Connected TV campaign performance metrics including impressions, completion rates, click-through rates, 
    and conversions for specific ad_slot or time periods. Supports filtering by campaign name (ad_slot), 
    platform, date ranges, and specific metrics. Returns detailed performance data with calculated metrics.
    Use this when users ask about campaign performance, completion rates, CTR, or conversion analysis.
    """
)
def get_platform_performance(
    ad_slot: Optional[str] = None,
    platform: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    time_operator: Literal['>', '<', '>=', '<=', '=', 'between'] = 'between',
    metrics: Optional[List[str]] = ['impressions', 'completion_rate', 'click_through_rate'],
    limit: Optional[int] = 10
) -> str:
    """
    Get Connected TV campaign performance data with flexible filtering
    
    Args:
        ad_slot: Filter by specific ad slot/campaign name (partial match supported)
        platform: Filter by platform (Roku, Hulu, Amazon Fire, Netflix)
        date_from: Start date (YYYY-MM-DD format)
        date_to: End date (YYYY-MM-DD format) 
        time_operator: How to apply date filtering
        metrics: List of metrics to return ['impressions', 'completion_rate', 'click_through_rate', 'conversions']
        limit: Maximum number of results to return
    """
    connection = None
    cursor = None
    
    try:
        # Build WHERE conditions
        where_conditions = []
        params = []
        
        if ad_slot:
            where_conditions.append("LOWER(ad_slot) LIKE LOWER(%s)")
            params.append(f"%{ad_slot}%")
        
        if platform:
            where_conditions.append("LOWER(platform) = LOWER(%s)")
            params.append(platform)
        
        # Date filtering
        if date_from and date_to and time_operator == 'between':
            where_conditions.append("date BETWEEN %s AND %s")
            params.extend([date_from, date_to])
        elif date_from:
            where_conditions.append(f"date {time_operator} %s")
            params.append(date_from)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build SELECT clause
        if metrics:
            metric_columns = ', '.join(metrics)
            select_clause = f"date, platform, ad_slot, {metric_columns}"
        else:
            select_clause = "date, platform, ad_slot, impressions, completion_rate, click_through_rate, conversions"
        
        # Build base query with calculated metrics
        query = f"""
        SELECT {select_clause},
               ROUND(completion_rate * 100, 2) as completion_rate_percent,
               ROUND(click_through_rate * 100, 2) as ctr_percent,
               ROUND((conversions::DECIMAL / NULLIF(impressions, 0)) * 100, 4) as conversion_rate,
               ROUND(conversions::DECIMAL / NULLIF(impressions, 0) * click_through_rate, 6) as engagement_score
        FROM {TABLE_NAME} 
        {where_clause}
        ORDER BY impressions DESC
        LIMIT %s
        """
        
        params.append(limit)
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        # Execute main query
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        platform_results = cursor.fetchall()
        
        # Get summary statistics
        summary_where_clause = where_clause
        summary_params = params[:-1]  # Remove limit parameter
        
        summary_query = f"""
        SELECT 
            COUNT(*) as total_ad_slots,
            SUM(impressions) as total_impressions,
            SUM(conversions) as total_conversions,
            ROUND(AVG(completion_rate), 4) as avg_completion_rate,
            ROUND(AVG(click_through_rate), 4) as avg_ctr,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 4) as overall_conversion_rate,
            COUNT(DISTINCT platform) as platform_count,
            MIN(date) as start_date,
            MAX(date) as end_date
        FROM {TABLE_NAME}
        {summary_where_clause}
        """
        
        cursor.execute(summary_query, summary_params)
        summary_results = cursor.fetchall()
        summary_data = summary_results[0] if summary_results else {}
        
        # Format output
        output = "Connected TV platform Performance Report\n" + "=" * 60 + "\n\n"
        
        if summary_data:
            output += (
                f"Summary Statistics:\n"
                f"Total ad_slots: {summary_data.get('total_ad_slots', 0):,}\n"
                f"Date Range: {summary_data.get('start_date', 'N/A')} to {summary_data.get('end_date', 'N/A')}\n"
                f"Total Impressions: {summary_data.get('total_impressions', 0):,}\n"
                f"Total Conversions: {summary_data.get('total_conversions', 0):,}\n"
                f"Average Completion Rate: {(summary_data.get('avg_completion_rate', 0) * 100):.2f}%\n"
                f"Average Click-Through Rate: {(summary_data.get('avg_ctr', 0) * 100):.2f}%\n"
                f"Overall Conversion Rate: {summary_data.get('overall_conversion_rate', 0):.4f}%\n"
                f"Platforms: {summary_data.get('platform_count', 0)}\n\n"
            )
        
        if campaign_results:
            output += f"Campaign Details ({len(campaign_results)} ad_slot):\n" + "-" * 60 + "\n"
            for row in campaign_results:
                output += (
                    f"Date: {row['date']}\n"
                    f"Platform: {row['platform']}\n"
                    f"Ad Slot: {row['ad_slot']}\n"
                    f"Impressions: {row['impressions']:,}\n"
                    f"Completion Rate: {row.get('completion_rate_percent', 0)}%\n"
                    f"Click-Through Rate: {row.get('ctr_percent', 0)}%\n"
                    f"Conversions: {row['conversions']}\n"
                    f"Conversion Rate: {row.get('conversion_rate', 0):.4f}%\n"
                    f"Engagement Score: {row.get('engagement_score', 0):.6f}\n"
                    + "-" * 60 + "\n"
                )
        else:
            output += "No campaign data found matching the criteria.\n"
        
        return output
        
    except Exception as e:
        return f"Error processing request: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)



@tool(
    name="get_platform_trends", 
    description="""
    Analyze Connected TV campaign performance trends over time with flexible time periods and metrics.
    Supports daily, weekly, or monthly aggregation for impressions, completion rates, CTR, and conversions.
    Shows trend direction and percentage changes over time for Connected TV specific metrics.
    Use this when users ask about performance trends, seasonal patterns, or time-based analysis.
    """
)
def get_platform_trends(
    ad_slot: Optional[str] = None,
    platform: Optional[str] = None,
    period: Literal['daily', 'weekly', 'monthly'] = 'daily',
    metric: Literal['impressions', 'completion_rate', 'click_through_rate', 'conversions'] = 'impressions',
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    days_back: Optional[int] = 30
) -> str:
    """
    Analyze trends in Connected TV campaign performance over time
    
    Args:
        ad_slot: Filter by specific ad slot/campaign name
        platform: Filter by platform (Roku, Hulu, Amazon Fire, Netflix)
        period: Aggregation period (daily, weekly, monthly)
        metric: Metric to analyze trends for
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        days_back: Number of days to look back if no date range specified
    """
    connection = None
    cursor = None
    
    try:
        # Build WHERE conditions
        where_conditions = []
        params = []
        
        if ad_slot:
            where_conditions.append("LOWER(ad_slot) LIKE LOWER(%s)")
            params.append(f"%{ad_slot}%")
        
        if platform:
            where_conditions.append("LOWER(platform) = LOWER(%s)")
            params.append(platform)
        
        if date_from:
            where_conditions.append("date >= %s")
            params.append(date_from)
        elif not date_to:  # If no date range specified, use days_back
            where_conditions.append(f"date >= CURRENT_DATE - INTERVAL '{days_back} days'")
        
        if date_to:
            where_conditions.append("date <= %s")
            params.append(date_to)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
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
            SUM(conversions) as conversions,
            ROUND(AVG(completion_rate), 4) as avg_completion_rate,
            ROUND(AVG(click_through_rate), 4) as avg_click_through_rate,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 4) as conversion_rate,
            COUNT(*) as campaign_count
        FROM {TABLE_NAME} 
        {where_clause}
        GROUP BY {period_group}
        ORDER BY {period_group}
        """
        
        print(f"Executing trends query: {query}")
        print(f"Parameters: {params}")
        
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        trend_results = cursor.fetchall()
        
        if not trend_results:
            return f"No trend data found for the specified criteria."
        
        output = f"Connected TV Campaign Trends Analysis ({period.title()})\n" + "=" * 60 + "\n\n"
        
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
                f"Conversions: {row['conversions']:,} | "
                f"Completion Rate: {(row['avg_completion_rate'] * 100):.2f}% | "
                f"CTR: {(row['avg_click_through_rate'] * 100):.2f}% | "
                f"Conversion Rate: {row['conversion_rate']:.4f}% | "
                f"Campaigns: {row['campaign_count']}\n"
            )
        
        return output
        
    except Exception as e:
        return f"Error processing trends: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)


@tool(
    name="search_similar_ad_slot",
    description="""
    Search for Connected TV ad_slot by ad slot name or get ad_slot from specific time periods with advanced filtering.
    Supports filtering by platform, impression thresholds, conversion requirements, and campaign performance criteria.
    Returns aggregated campaign data grouped by ad slot with performance metrics including completion rates and CTR.
    Use this when users want to find specific ad_slot or explore what ad_slot were running on Connected TV platforms.
    """
)
def search_similar_ad_slot(
    search_term: Optional[str] = None,
    platform: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    min_impressions: Optional[int] = None,
    has_conversions: Optional[bool] = None,
    limit: Optional[int] = 10
) -> str:
    """
    Search and filter Connected TV ad_slot based on various criteria
    
    Args:
        search_term: Search term to find in ad slot names
        platform: Filter by platform (Roku, Hulu, Amazon Fire, Netflix)
        date_from: Start date filter (YYYY-MM-DD)
        date_to: End date filter (YYYY-MM-DD)
        min_impressions: Minimum total impressions filter
        has_conversions: Filter ad_slot that have/don't have conversions
        limit: Maximum number of results to return
    """
    connection = None
    cursor = None
    
    try:
        # Build WHERE conditions
        where_conditions = []
        params = []
        
        if search_term:
            where_conditions.append("ad_slot ILIKE %s")
            params.append(f"%{search_term}%")
        
        if platform:
            where_conditions.append("LOWER(platform) = LOWER(%s)")
            params.append(platform)
        
        if date_from:
            where_conditions.append("date >= %s")
            params.append(date_from)
            
        if date_to:
            where_conditions.append("date <= %s")
            params.append(date_to)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build HAVING conditions for aggregated filters
        having_conditions = []
        if min_impressions:
            having_conditions.append("SUM(impressions) >= %s")
            params.append(min_impressions)
        
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
            ad_slot,
            platform,
            SUM(impressions) as total_impressions,
            SUM(conversions) as total_conversions,
            ROUND(AVG(completion_rate), 4) as avg_completion_rate,
            ROUND(AVG(click_through_rate), 4) as avg_click_through_rate,
            MIN(date) as start_date,
            MAX(date) as end_date,
            COUNT(DISTINCT date) as days_active,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 4) as conversion_rate,
            ROUND(AVG(completion_rate) * AVG(click_through_rate), 6) as engagement_score
        FROM {TABLE_NAME}
        {where_clause} 
        GROUP BY ad_slot, platform
        {having_clause}
        ORDER BY total_impressions DESC
        LIMIT %s
        """
        
        params.append(limit)
        
        print(f"Executing search ad_slot query: {query}")
        print(f"Parameters: {params}")
        
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        similar_results = cursor.fetchall()
        
        # output = ""
        
        # if similar_results:
        #     output += f"Ad Slot Details ({len(similar_results)} results):\n" + "-" * 60 + "\n"
            
        #     for result in similar_results:
        #         output += (
        #             f"Ad Slot: {result.get('ad_slot', 'N/A')}\n"
        #             f"Platform: {result.get('platform', 'N/A')}\n"
        #             f"Period: {result.get('start_date', 'N/A')} to {result.get('end_date', 'N/A')} ({result.get('days_active', 0)} days)\n"
        #             f"Total Impressions: {result.get('total_impressions', 0):,}\n"
        #             f"Total Conversions: {result.get('total_conversions', 0):,}\n"
        #             f"Avg Completion Rate: {(result.get('avg_completion_rate', 0) * 100):.2f}%\n"
        #             f"Avg CTR: {(result.get('avg_click_through_rate', 0) * 100):.2f}%\n"
        #             f"Conversion Rate: {result.get('conversion_rate', 0):.4f}%\n"
        #             f"Engagement Score: {result.get('engagement_score', 0):.6f}\n"
        #             + "-" * 60 + "\n"
        #         )
        # else:
        #     output += "No ad_slot data found matching the criteria.\n"
        
        if similar_results:
            return similar_results
        else:
            return "No similar result has been found"

       
        
    except Exception as e:
        return f"Error searching ad_slot: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)
