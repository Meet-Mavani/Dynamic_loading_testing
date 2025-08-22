from typing import Optional,Literal,List
from strands import Agent, tool
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import Error


import psycopg2.pool

# Database configuration - update with your Aurora PostgreSQL details
DB_CONFIG = {
    "host": "secai-database.cluster-c4timsc6k2gq.us-east-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "h9hS5I9eaB5Vyot2kU03w|3P?nC_",
    "database": "testing_db",
    "port": 5432
}

TABLE_NAME = "linkedin_ads"  

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


@tool(name="linkedin_ads_agent", description=(
        "An intelligent LinkedIn Ads analytics agent that interprets natural language queries "
        "and routes them to the appropriate sub-tools for detailed insights. "
        "It provides campaign performance reports, time-based trend analysis, and advanced campaign search. \n\n"
        "Capabilities include:\n"
        "- **Campaign performance reports**: Impressions, clicks, spend, conversions, CTR, CPC, conversion rates, ROAS, and cost efficiency.\n"
        "- **Trend analysis**: Daily, weekly, or monthly breakdowns of impressions, clicks, spend, conversions, CTR, and CPC with trend directions.\n"
        "- **Campaign search & discovery**: Find campaigns by name, ID, date ranges, spend thresholds, conversions, and engagement metrics.\n\n"
        "Use this tool whenever users ask broad questions about LinkedIn Ads, want campaign comparisons, "
        "trend insights, or need to discover campaigns without specifying the exact sub-tool. "
        "All responses are returned in markdown format with key insights clearly highlighted."
    ))
def linkedin_ads_agent(prompt: str) -> str:
    """Main LinkedIn ads agent tool that routes to appropriate sub-tools"""
    supervisor_agent = Agent(
        tools=[linkedin_search_similar_campaigns, linkedin_get_campaign_performance, linkedin_get_campaign_trends]
    )
    
    response = supervisor_agent(prompt)
    return str(response)

# -------------------------------------------------------------------
# Tools
# -------------------------------------------------------------------



@tool(
    name="linkedin_get_campaign_performance", 
    description="""
    Get LinkedIn campaign performance metrics including impressions, clicks, spend, conversions,
    click-through rates (CTR), cost per click (CPC), and conversion rates for specific campaigns or time periods.
    Supports filtering by campaign name, campaign ID, date ranges, and specific metrics.
    Returns detailed performance data with calculated metrics like CTR, CPC, conversion rates, and ROAS.
    Use this when users ask about campaign performance, CTR, CPC, conversion analysis, or spend efficiency.
    """
)
def linkedin_get_campaign_performance(
    campaign_name: Optional[str] = None,
    campaign_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    time_operator: Literal['>', '<', '>=', '<=', '=', 'between'] = 'between',
    metrics: Optional[List[str]] = ['impressions', 'clicks', 'spend', 'conversions'],
    limit: Optional[int] = 10
) -> str:
    """
    Get LinkedIn campaign performance data with flexible filtering
    
    Args:
        campaign_name: Filter by specific campaign name (partial match supported)
        campaign_id: Filter by specific campaign ID
        date_from: Start date (YYYY-MM-DD format)
        date_to: End date (YYYY-MM-DD format) 
        time_operator: How to apply date filtering
        metrics: List of metrics to return ['impressions', 'clicks', 'spend', 'conversions']
        limit: Maximum number of results to return
    """
    connection = None
    cursor = None
    
    try:
        # Build WHERE conditions
        where_conditions = []
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
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build SELECT clause
        if metrics:
            metric_columns = ', '.join(metrics)
            select_clause = f"date, campaign_id, campaign_name, {metric_columns}"
        else:
            select_clause = "date, campaign_id, campaign_name, impressions, clicks, spend, conversions"
        
        # Build base query with calculated metrics
        query = f"""
        SELECT {select_clause},
               ROUND((clicks::DECIMAL / NULLIF(impressions, 0)) * 100, 2) as ctr_percent,
               ROUND(spend / NULLIF(clicks, 0), 2) as cpc,
               ROUND((conversions::DECIMAL / NULLIF(impressions, 0)) * 100, 4) as conversion_rate,
               ROUND(spend / NULLIF(conversions, 0), 2) as cost_per_conversion,
               ROUND((conversions::DECIMAL / NULLIF(clicks, 0)) * 100, 2) as click_to_conversion_rate
        FROM {TABLE_NAME} 
        {where_clause}
        ORDER BY spend DESC
        LIMIT %s
        """
        
        params.append(limit)
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        # Execute main query
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        campaign_results = cursor.fetchall()
        
        # Get summary statistics
        summary_where_clause = where_clause
        summary_params = params[:-1]  # Remove limit parameter
        
        summary_query = f"""
        SELECT 
            COUNT(*) as total_campaigns,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(spend) as total_spend,
            SUM(conversions) as total_conversions,
            ROUND(AVG(clicks::DECIMAL / NULLIF(impressions, 0)), 4) as avg_ctr,
            ROUND(AVG(spend / NULLIF(clicks, 0)), 2) as avg_cpc,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 4) as overall_conversion_rate,
            ROUND(SUM(spend) / NULLIF(SUM(conversions), 0), 2) as avg_cost_per_conversion,
            MIN(date) as start_date,
            MAX(date) as end_date
        FROM {TABLE_NAME}
        {summary_where_clause}
        """
        
        cursor.execute(summary_query, summary_params)
        summary_results = cursor.fetchall()
        summary_data = summary_results[0] if summary_results else {}
        
        # Format output
        output = "LinkedIn Campaign Performance Report\n" + "=" * 60 + "\n\n"
        
        if summary_data and campaign_results:
            return campaign_results,summary_data
        else:
            return campaign_results

            # output += (
            #     f"Summary Statistics:\n"
            #     f"Total Campaigns: {summary_data.get('total_campaigns', 0):,}\n"
            #     f"Date Range: {summary_data.get('start_date', 'N/A')} to {summary_data.get('end_date', 'N/A')}\n"
            #     f"Total Impressions: {summary_data.get('total_impressions', 0):,}\n"
            #     f"Total Clicks: {summary_data.get('total_clicks', 0):,}\n"
            #     f"Total Spend: ${summary_data.get('total_spend', 0):,.2f}\n"
            #     f"Total Conversions: {summary_data.get('total_conversions', 0):,}\n"
            #     f"Average CTR: {(summary_data.get('avg_ctr', 0) * 100):.2f}%\n"
            #     f"Average CPC: ${summary_data.get('avg_cpc', 0):.2f}\n"
            #     f"Overall Conversion Rate: {summary_data.get('overall_conversion_rate', 0):.4f}%\n"
            #     f"Average Cost per Conversion: ${summary_data.get('avg_cost_per_conversion', 0):.2f}\n\n"
            # )
        
        # if campaign_results:
            # output += f"Campaign Details ({len(campaign_results)} campaigns):\n" + "-" * 60 + "\n"
            # for row in campaign_results:
            #     output += (
            #         f"Date: {row['date']}\n"
            #         f"Campaign ID: {row['campaign_id']}\n"
            #         f"Campaign Name: {row['campaign_name']}\n"
            #         f"Impressions: {row['impressions']:,}\n"
            #         f"Clicks: {row['clicks']:,}\n"
            #         f"Spend: ${row['spend']:,.2f}\n"
            #         f"Conversions: {row['conversions']}\n"
            #         f"CTR: {row.get('ctr_percent', 0)}%\n"
            #         f"CPC: ${row.get('cpc', 0):.2f}\n"
            #         f"Conversion Rate: {row.get('conversion_rate', 0):.4f}%\n"
            #         f"Cost per Conversion: ${row.get('cost_per_conversion', 0):.2f}\n"
            #         f"Click-to-Conversion Rate: {row.get('click_to_conversion_rate', 0)}%\n"
            #         + "-" * 60 + "\n"
            #     )
        # else:
        #     output += "No campaign data found matching the criteria.\n"
        
        # return output
        
    except Exception as e:
        return f"Error processing request: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)

@tool(
    name="linkedin_get_campaign_trends", 
    description="""
    Analyze LinkedIn campaign performance trends over time with flexible time periods and metrics.
    Supports daily, weekly, or monthly aggregation for impressions, clicks, spend, conversions, CTR, and CPC.
    Shows trend direction and percentage changes over time for LinkedIn-specific metrics.
    Use this when users ask about performance trends, seasonal patterns, spend patterns, or time-based analysis.
    """
)
def linkedin_get_campaign_trends(
    campaign_name: Optional[str] = None,
    period: Literal['daily', 'weekly', 'monthly'] = 'daily',
    metric: Literal['impressions', 'clicks', 'spend', 'conversions', 'ctr', 'cpc'] = 'impressions',
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    days_back: Optional[int] = 30
) -> str:
    """
    Analyze trends in LinkedIn campaign performance over time
    
    Args:
        campaign_name: Filter by specific campaign name
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
        
        if campaign_name:
            where_conditions.append("LOWER(campaign_name) LIKE LOWER(%s)")
            params.append(f"%{campaign_name}%")
        
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
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            SUM(conversions) as conversions,
            ROUND(AVG(clicks::DECIMAL / NULLIF(impressions, 0)), 4) as avg_ctr,
            ROUND(AVG(spend / NULLIF(clicks, 0)), 2) as avg_cpc,
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
        
        if trend_results:
            return trend_results
        else:
            return f"No trend data found for the specified criteria."
        
        # output = f"LinkedIn Campaign Trends Analysis ({period.title()})\n" + "=" * 60 + "\n\n"
        
        # if len(trend_results) > 1:
        #     # Handle metric-specific trend calculation
        #     if metric == 'ctr':
        #         first_value = float(trend_results[0].get('avg_ctr', 0))
        #         last_value = float(trend_results[-1].get('avg_ctr', 0))
        #     elif metric == 'cpc':
        #         first_value = float(trend_results[0].get('avg_cpc', 0))
        #         last_value = float(trend_results[-1].get('avg_cpc', 0))
        #     else:
        #         first_value = float(trend_results[0].get(metric, 0))
        #         last_value = float(trend_results[-1].get(metric, 0))
            
        #     if first_value > 0:
        #         percent_change = ((last_value - first_value) / first_value) * 100
        #         trend_direction = "increasing" if last_value > first_value else "decreasing"
        #         output += f"Trend Direction for {metric.upper()}: {trend_direction.title()} ({percent_change:+.1f}%)\n"
        #         output += f"Periods Analyzed: {len(trend_results)}\n\n"
        
        # # Display trend data
        # for row in trend_results:
        #     output += (
        #         f"{row['period']}: "
        #         f"Impressions: {row['impressions']:,} | "
        #         f"Clicks: {row['clicks']:,} | "
        #         f"Spend: ${row['spend']:,.2f} | "
        #         f"Conversions: {row['conversions']:,} | "
        #         f"CTR: {(row['avg_ctr'] * 100):.2f}% | "
        #         f"CPC: ${row['avg_cpc']:.2f} | "
        #         f"Conv. Rate: {row['conversion_rate']:.4f}% | "
        #         f"Campaigns: {row['campaign_count']}\n"
        # #     )
        
        # return output
        
    except Exception as e:
        return f"Error processing trends: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)

@tool(
    name="linkedin_search_similar_campaigns",
    description="""
    Search for LinkedIn campaigns by campaign name or get campaigns from specific time periods with advanced filtering.
    Supports filtering by spend thresholds, conversion requirements, and campaign performance criteria.
    Returns aggregated campaign data grouped by campaign with performance metrics including CTR, CPC, and conversion rates.
    Use this when users want to find specific campaigns or explore what campaigns were running on LinkedIn.
    """
)
def linkedin_search_similar_campaigns(
    search_term: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    min_spend: Optional[float] = None,
    min_impressions: Optional[int] = None,
    has_conversions: Optional[bool] = None,
    limit: Optional[int] = 20  # Add the missing limit parameter
) -> str:
    """
    Search and filter LinkedIn campaigns based on various criteria
    
    Args:
        search_term: Search term to find in campaign names
        date_from: Start date filter (YYYY-MM-DD)
        date_to: End date filter (YYYY-MM-DD)
        min_spend: Minimum total spend filter
        min_impressions: Minimum total impressions filter
        has_conversions: Filter campaigns that have/don't have conversions
        limit: Maximum number of results to return
    """
    connection = None
    cursor = None  # Add cursor initialization
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Build WHERE conditions
        where_conditions = []
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
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build HAVING conditions
        having_conditions = []

        if min_spend:
            having_conditions.append("SUM(spend) >= %s")
            params.append(min_spend)
            
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
        
        # Fixed SQL query - removed trailing comma and added calculated metrics
        query = f"""
        SELECT 
            campaign_name,
            campaign_id,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(spend) as total_spend,
            SUM(conversions) as total_conversions,
            ROUND((SUM(clicks)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 2) as avg_ctr,
            ROUND(SUM(spend) / NULLIF(SUM(clicks), 0), 2) as avg_cpc,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 4) as conversion_rate,
            ROUND(SUM(spend) / NULLIF(SUM(conversions), 0), 2) as cost_per_conversion,
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(*) as total_records
        FROM {TABLE_NAME}
        {where_clause} 
        GROUP BY campaign_name, campaign_id
        {having_clause}
        ORDER BY total_spend DESC
        LIMIT %s
        """
        
        params.append(limit)
        
        print(f"Executing search campaigns query: {query}")
        print(f"Parameters: {params}")

        cursor.execute(query, params)
        similar_campaign_results = cursor.fetchall()

        # print(f"Found {len(campaign_results)} campaigns")
        # print(campaign_results)
        # output = f"Campaign Search Results\n" + "=" * 60 + "\n\n"
        
        if similar_campaign_results:
            # output += f"Found {len(campaign_results)} campaign(s) matching your criteria:\n" + "-" * 60 + "\n"
            
            # for result in campaign_results:
            #     output += (
            #         f"Campaign Name: {result.get('campaign_name', 'N/A')}\n"
            #         f"Campaign ID: {result.get('campaign_id', 'N/A')}\n"
            #         f"Date Range: {result.get('first_date', 'N/A')} to {result.get('last_date', 'N/A')}\n"
            #         f"Total Records: {result.get('total_records', 0):,}\n"
            #         f"Total Impressions: {result.get('total_impressions', 0):,}\n"
            #         f"Total Clicks: {result.get('total_clicks', 0):,}\n"
            #         f"Total Spend: ${result.get('total_spend', 0):,.2f}\n"
            #         f"Total Conversions: {result.get('total_conversions', 0):,}\n"
            #         f"Average CTR: {result.get('avg_ctr', 0):.2f}%\n"
            #         f"Average CPC: ${result.get('avg_cpc', 0):.2f}\n"
            #         f"Conversion Rate: {result.get('conversion_rate', 0):.4f}%\n"
               
            #         + "-" * 60 + "\n"
            #     ) 
            return similar_campaign_results
        else:
            return "No campaigns found matching the specified criteria.\n"
        # return output  # Return string directly, not dict  
    except Exception as e:
        return f"Error searching campaigns: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)
