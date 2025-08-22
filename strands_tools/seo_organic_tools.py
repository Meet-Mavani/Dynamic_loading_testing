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


TABLE_NAME = "seo_organic_ads"  

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
# Merged Web Analytics Tools
# -------------------------------------------------------------------

@tool(name="get_page_analytics",
    description=(
        "Retrieve detailed page-level SEO analytics from the organic ads dataset. "
        "This tool allows filtering results by date range, page URL keyword, sessions, "
        "conversions, bounce rate, and traffic source. It returns granular metrics for "
        "each matching page, including:\n\n"
        "- Date of record (ad_date)\n"
        "- Page URL\n"
        "- Sessions & unique visitors (with '%' of unique visitors)\n"
        "- Bounce rate (percentage)\n"
        "- Average session duration (in seconds and minutes)\n"
        "- Conversions & conversion rate (percentage)\n"
        "- Traffic source\n\n"
        "Results can be sorted by metrics such as sessions, conversions, conversion rate, "
        "bounce rate, or average session duration, with a configurable limit on the number "
        "of results returned. This tool is useful for ranking, filtering, and analyzing "
        "specific pages' performance over time."
    )
)

def get_page_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page_url: Optional[str] = None,
    min_sessions: Optional[int] = None,
    max_bounce_rate: Optional[float] = None,
    min_conversions: Optional[int] = None,
    source: Optional[str] = None,
    sort_by: Optional[str] = "date",
    sort_order: Optional[str] = "desc",
    limit: Optional[int] = 10,
) -> str:
    """
    tool for page performance analysis - combines page performance, search, and ranking capabilities.
    
    Args:
        start_date: Filter by start date (YYYY-MM-DD format)
        end_date: Filter by end date (YYYY-MM-DD format)
        page_url: Search for pages containing this keyword in URL
        min_sessions: Minimum number of sessions
        max_bounce_rate: Maximum bounce rate (as decimal, e.g., 0.5 for 50%)
        min_conversions: Minimum number of conversions
        source: Filter by traffic source
        sort_by: Sort by metric (date, sessions, conversions, conversion_rate, bounce_rate, avg_session_duration)
        sort_order: Sort order (asc, desc)
        limit: Number of results to return
    """
    connection = None
    try:
        connection = get_db_connection()

        cursor = connection.cursor(cursor_factory=RealDictCursor)

        query = f"""
            SELECT ad_date, page_url, sessions, unique_visitors, 
                   ROUND(bounce_rate * 100, 2) as bounce_rate_pct,
                   avg_session_duration_sec,
                   ROUND(avg_session_duration_sec / 60.0, 2) as avg_session_duration_min,
                   conversions, source,
                   ROUND(
                       CAST(conversions * 100.0 / NULLIF(sessions, 0) AS NUMERIC), 
                       2
                   ) as conversion_rate_pct,
                   ROUND(
                       CAST(unique_visitors * 100.0 / NULLIF(sessions, 0) AS NUMERIC), 
                       2
                   ) as unique_visitor_rate_pct
            FROM {TABLE_NAME}
        """

        where_conditions = []
        params = []
        
        if page_url:
            where_conditions.append("page_url ILIKE %s")
            params.append(f"%{page_url}%")
        if start_date:
            where_conditions.append("ad_date >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("ad_date <= %s")
            params.append(end_date)
        if min_sessions is not None:
            where_conditions.append("sessions >= %s")
            params.append(min_sessions)
        if max_bounce_rate is not None:
            where_conditions.append("bounce_rate <= %s")
            params.append(max_bounce_rate)
        if min_conversions is not None:
            where_conditions.append("conversions >= %s")
            params.append(min_conversions)
        if source:
            where_conditions.append("source ILIKE %s")
            params.append(f"%{source}%")

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        # Handle sorting
        sort_mapping = {
            "ad_date": "ad_date",
            "sessions": "sessions",
            "conversions": "conversions", 
            "conversion_rate": "conversions * 100.0 / NULLIF(sessions, 0)",
            "bounce_rate": "bounce_rate",
            "avg_session_duration": "avg_session_duration_sec",
            "unique_visitors": "unique_visitors"
        }
        
        if sort_by in sort_mapping:
            if sort_by == "bounce_rate" and sort_order.lower() == "desc":
                query += f" ORDER BY {sort_mapping[sort_by]} ASC NULLS LAST"  # Lower bounce rate is better
            else:
                query += f" ORDER BY {sort_mapping[sort_by]} {sort_order.upper()} NULLS LAST"
        else:
            query += " ORDER BY ad_date DESC"

        query += " LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()

        if not results:
            return "No pages found matching the criteria."
        else:
            return results

        # # Dynamic header based on search/filter context
        # if page_url:
        #     output = f"Found {len(results)} pages with '{page_url}' in URL:\n\n"
        # elif sort_by != "date":
        #     output = f"Top {len(results)} pages by {sort_by.replace('_', ' ').title()}:\n\n"
        # else:
        #     output = f"Page Analytics Results ({len(results)} records):\n\n"

        # for i, row in enumerate(results, 1):
        #     if sort_by != "date":
        #         output += f"{i}. "
            
        #     output += (
        #         f"Date: {row['date']} | Page: {row['page_url']}\n"
        #         f"   Sessions: {row['sessions']:,} | Unique Visitors: {row['unique_visitors']:,} ({row['unique_visitor_rate_pct']}%)\n"
        #         f"   Bounce Rate: {row['bounce_rate_pct']}% | Avg Duration: {row['avg_session_duration_min']} min\n"
        #         f"   Conversions: {row['conversions']} ({row['conversion_rate_pct'] or 0}%)\n"
        #         f"   Source: {row['source']}\n"
        #         + "-" * 60 + "\n"
        #     )

        # return output

    except Exception as e:
        return f"Error retrieving page analytics: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)


@tool(name="get_analytics_summary",description="Generates a comprehensive SEO analytics summary from organic ads data. This tool aggregates performance metrics at different levels depending on the 'group_by' parameter: 'overall': Returns total sessions, unique visitors, bounce rate, conversions, average session duration, and overall conversion rates across the selected date range. 'source': Breaks down performance by traffic source, showing sessions, unique visitors, bounce rates, ""and conversion performance for each source.  'daily_trends': Provides daily trends over a configurable number of days, showing sessions, visitors, conversions, bounce rates, and average session duration per day. Filters can be applied by date range and traffic source.")
def get_analytics_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    source: Optional[str] = None,
    group_by: Optional[str] = "overall",
    days_for_trends: Optional[int] = 30
) -> str:
    """
    Comprehensive analytics summary combining overall KPIs, source performance, and trends analysis.
    
    Args:
        start_date: Filter by start date (YYYY-MM-DD format)
        end_date: Filter by end date (YYYY-MM-DD format)  
        source: Filter by specific traffic source
        group_by: Grouping option (overall, source, daily_trends)
        days_for_trends: Number of days for trend analysis (when group_by='daily_trends')
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        if group_by == "daily_trends":
            # Daily trends analysis
            query = f"""
                SELECT 
                    ad_date,
                    COUNT(*) as daily_pages,
                    SUM(sessions) as daily_sessions,
                    SUM(unique_visitors) as daily_unique_visitors,
                    ROUND(AVG(bounce_rate) * 100, 2) as avg_bounce_rate,
                    ROUND(AVG(avg_session_duration_sec) / 60.0, 2) as avg_session_duration_min,
                    SUM(conversions) as daily_conversions,
                    ROUND(
                        CAST(SUM(conversions) * 100.0 / NULLIF(SUM(sessions), 0) AS NUMERIC), 
                        2
                    ) as daily_conversion_rate
                FROM {TABLE_NAME}
                WHERE ad_date >= CURRENT_DATE - INTERVAL '{days_for_trends} DAYS'
            """
            
            params = []
            if source:
                query += " AND source ILIKE %s"
                params.append(f"%{source}%")
                
            query += """
                GROUP BY ad_date
                ORDER BY ad_date DESC
            """
            cursor.execute(query, params)
            results = cursor.fetchall()

            if not results:
                return f"No trend data available for the last {days_for_trends} days."
            else:
                return results

            # output = f"Traffic Trends (Last {days_for_trends} Days)\n" + "=" * 50 + "\n\n"
            # for row in results:
            #     output += (
            #         f"{row['date']}: "
            #         f"{row['daily_pages']} pages, "
            #         f"{row['daily_sessions']:,} sessions, "
            #         f"{row['daily_unique_visitors']:,} visitors, "
            #         f"{row['avg_bounce_rate']}% bounce, "
            #         f"{row['avg_session_duration_min']}min duration, "
            #         f"{row['daily_conversions']} conversions ({row['daily_conversion_rate'] or 0}%)\n"
            #     )

        elif group_by == "source":
            # Source performance analysis
            query = f"""
                SELECT 
                    source,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT page_url) as unique_pages,
                    SUM(sessions) as total_sessions,
                    SUM(unique_visitors) as total_unique_visitors,
                    ROUND(AVG(bounce_rate) * 100, 2) as avg_bounce_rate,
                    ROUND(AVG(avg_session_duration_sec) / 60.0, 2) as avg_session_duration_min,
                    SUM(conversions) as total_conversions,
                    ROUND(
                        CAST(SUM(conversions) * 100.0 / NULLIF(SUM(sessions), 0) AS NUMERIC), 
                        2
                    ) as conversion_rate
                FROM {TABLE_NAME}
            """

            where_conditions = []
            params = []
            
            if start_date:
                where_conditions.append("date >= %s")
                params.append(start_date)
            if end_date:
                where_conditions.append("date <= %s")
                params.append(end_date)
            if source:
                where_conditions.append("source ILIKE %s")
                params.append(f"%{source}%")

            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)

            query += """
                GROUP BY source
                ORDER BY SUM(sessions) DESC
            """

            cursor.execute(query, params)
            results = cursor.fetchall()
            if not results:
                return f"No trend data available for the source {source} days."
            else:
                return results


            # output = f"Traffic Source Performance Analysis:\n" + "=" * 50 + "\n\n"
            # for i, row in enumerate(results, 1):
            #     output += (
            #         f"{i}. {row['source']}\n"
            #         f"   Records: {row['total_records']:,} | Unique Pages: {row['unique_pages']:,}\n"
            #         f"   Sessions: {row['total_sessions']:,} | Unique Visitors: {row['total_unique_visitors']:,}\n"
            #         f"   Bounce Rate: {row['avg_bounce_rate']}% | Avg Duration: {row['avg_session_duration_min']} min\n"
            #         f"   Conversions: {row['total_conversions']:,} ({row['conversion_rate'] or 0}%)\n"
            #         + "-" * 50 + "\n"
            #     )

        else:
            # Overall summary
            query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT page_url) as unique_pages,
                    SUM(sessions) as total_sessions,
                    SUM(unique_visitors) as total_unique_visitors,
                    ROUND(AVG(bounce_rate) * 100, 2) as avg_bounce_rate,
                    ROUND(AVG(avg_session_duration_sec), 2) as avg_session_duration,
                    ROUND(AVG(avg_session_duration_sec) / 60.0, 2) as avg_session_duration_min,
                    SUM(conversions) as total_conversions,
                    ROUND(
                        CAST(SUM(conversions) * 100.0 / NULLIF(SUM(sessions), 0) AS NUMERIC), 
                        2
                    ) as overall_conversion_rate,
                    ROUND(
                        CAST(SUM(unique_visitors) * 100.0 / NULLIF(SUM(sessions), 0) AS NUMERIC), 
                        2
                    ) as unique_visitor_rate,
                    MIN(ad_date) as earliest_date,
                    MAX(ad_date) as latest_date
                FROM {TABLE_NAME}
            """
            where_conditions = []
            params = []
            
            if start_date:
                where_conditions.append("date >= %s")
                params.append(start_date)
            if end_date:
                where_conditions.append("date <= %s")
                params.append(end_date)
            if source:
                where_conditions.append("source ILIKE %s")
                params.append(f"%{source}%")

            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
      
            cursor.execute(query, params)
            result = cursor.fetchone()

            if not result or result["total_records"] == 0:
                return "No data found for the specified criteria."
            else:
                return result

        #     output = (
        #         "Web Analytics Overview\n"
        #         + "=" * 40 + "\n\n"
        #         f"Analysis Period: {result['earliest_date']} to {result['latest_date']}\n"
        #         f"Total Records: {result['total_records']:,}\n"
        #         f"Unique Pages: {result['unique_pages']:,}\n"
        #         f"Total Sessions: {result['total_sessions']:,}\n"
        #         f"Total Unique Visitors: {result['total_unique_visitors']:,}\n"
        #         f"Unique Visitor Rate: {result['unique_visitor_rate']}%\n"
        #         f"Average Bounce Rate: {result['avg_bounce_rate']}%\n"
        #         f"Average Session Duration: {result['avg_session_duration_min']} minutes\n"
        #         f"Total Conversions: {result['total_conversions']:,}\n"
        #         f"Overall Conversion Rate: {result['overall_conversion_rate'] or 0}%\n"
        #     )

        # return output

    except Exception as e:
        return f"Error generating analytics summary: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)


@tool(name="SEO_analytics_agent", description="An intelligent SEO analytics agent for analyzing organic traffic performance.This tool allows querying and summarizing SEO organic ads data stored in the Aurora PostgreSQL database. It supports filtering by date ranges, page URLs, sessions, conversions, traffic sources, and bounce rates. It can return detailed page-level performance metrics (sessions, unique visitors, bounce rate, conversion rate, average session duration, etc.) as well as aggregated summaries by source, overall performance, or daily trends. The agent always returns results in **markdown format**, highlighting **key insights** from the retrieved data. ")
def SEO_analytics_agent(prompt: str) -> str:
    """
    Intelligent web analytics analysis agent that orchestrates the merged analytics tools.
    """
    agent = Agent(system_prompt="You are a helpful Seo Analytics agent always gives result in markdown format. Always include the Key insights from the retrieved result",tools=[
        get_page_analytics,
        get_analytics_summary
    ])
    response = agent(prompt)
    return response


def cleanup_connections():
    """Clean up connection pool"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()


# SEO_analytics_agent("Give me analytics summary of the search/app page url")