from typing import Optional
from strands import Agent, tool
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import Error
import psycopg2.pool

# Database configuration - update with your Aurora PostgreSQL details
DB_CONFIG = {
    "host": "secai-database.cluster-c4timsc6k2gq.us-east-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "lgUKL_:1X|AIjqAC-jXuXW-6V7G9",
    "database": "testing_db",
    "port": 5432
}

TABLE_NAME = "email_campaigns"  # Update with your actual table name

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
# Tools
# -------------------------------------------------------------------

@tool
def get_campaign_performance(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_open_rate: Optional[float] = None,
    min_click_rate: Optional[float] = None,
    campaign_id: Optional[str] = None,
    limit: Optional[int] = 10,
) -> str:
    """
    Retrieve detailed email campaign performance data with advanced filtering options.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        query = f"""
            SELECT date, campaign_id, subject_line, emails_sent, 
                   ROUND(open_rate * 100, 2) as open_rate_pct,
                   ROUND(click_through_rate * 100, 2) as click_rate_pct,
                   unsubscribes, conversions, source,
                   ROUND(
                       CAST(conversions * 100.0 / NULLIF(emails_sent, 0) AS NUMERIC), 
                       2
                   ) as conversion_rate_pct
            FROM {TABLE_NAME}
        """

        where_conditions = []
        params = []
        if campaign_id:
            where_conditions.append(f"campaign_id = '{campaign_id}'")
            params.append(campaign_id)
        if start_date:
            where_conditions.append(f"date >= {start_date}")
            params.append(start_date)
        if end_date:
            where_conditions.append(f"date <= {end_date}")
            params.append(end_date)
        if min_open_rate is not None:
            where_conditions.append(f"open_rate >= {min_open_rate}")
            params.append(min_open_rate / 100.0)  # Convert percentage to decimal
        if min_click_rate is not None:
            where_conditions.append(f"click_through_rate >= {min_click_rate}")
            params.append(min_click_rate / 100.0)  # Convert percentage to decimal

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        query += f" ORDER BY date DESC LIMIT {limit}"
        params.append(limit)
        print("Queryyyyyyyyyyyyyyyyyyyy:     ",query)

        cursor.execute(query, params)
        print("11111111111111111111111111111")
        results = cursor.fetchall()
        print("Resulltttttttttttttttttttttttttttt: ",result)

        if not results:
            return "No campaigns found matching the criteria."

        output = f"Found {len(results)} campaigns:\n\n"
        for row in results:
            output += (
                f"Date: {row['date']}\n"
                f"Campaign ID: {row['campaign_id']}\n"
                f"Subject: {row['subject_line']}\n"
                f"Emails Sent: {row['emails_sent']:,}\n"
                f"Open Rate: {row['open_rate_pct']}%\n"
                f"Click Rate: {row['click_rate_pct']}%\n"
                f"Conversions: {row['conversions']} ({row['conversion_rate_pct'] or 0}%)\n"
                f"Unsubscribes: {row['unsubscribes']}\n"
                f"Source: {row['source']}\n"
                + "-" * 50 + "\n"
            )

        return output

    except Exception as e:
        return f"Error retrieving campaign data: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)


@tool
def get_campaign_performance(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    metric: Optional[str] = "conversion_rate",
    limit: Optional[int] = 3,
    campaign_id: Optional[str] = None,
) -> str:
    """
    Identify and rank the highest-performing email campaigns.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        metric_mapping = {
            "conversion_rate": "conversions * 100.0 / NULLIF(emails_sent, 0)",
            "open_rate": "open_rate * 100",
            "click_through_rate": "click_through_rate * 100",
        }

        if metric not in metric_mapping:
            return f"Invalid metric. Choose from: {', '.join(metric_mapping.keys())}"

        # Build the base query
        query = f"""
            SELECT date, campaign_id, subject_line, emails_sent,
                   ROUND(open_rate * 100, 2) as open_rate_pct,
                   ROUND(click_through_rate * 100, 2) as click_rate_pct,
                   conversions,
                   unsubscribes,
                   source,
                   ROUND(
                        CAST(conversions * 100.0 / NULLIF(emails_sent, 0) AS NUMERIC), 
                        2
                    ) AS conversion_rate_pct,
                   ROUND(
                        CAST({metric_mapping[metric]} AS NUMERIC), 
                        2
                    ) AS metric_value
            FROM {TABLE_NAME}
        """

        # Build WHERE conditions
        where_conditions = ["emails_sent > 0"]
        params = []
        
        if start_date:
            where_conditions.append("date >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date <= %s")
            params.append(end_date)
        if campaign_id:
            where_conditions.append("campaign_id = %s")
            params.append(campaign_id)

        query += " WHERE " + " AND ".join(where_conditions)

        # Add ORDER BY and LIMIT
        if metric == "conversion_rate":
            query += """
                ORDER BY 
                    (conversions * 100.0 / NULLIF(emails_sent, 0)) DESC NULLS LAST,
                    conversions DESC  
                LIMIT %s
            """
        elif metric == "open_rate":
            query += """
                ORDER BY 
                    open_rate DESC NULLS LAST,
                    emails_sent DESC  
                LIMIT %s
            """
        elif metric == "click_through_rate":
            query += """
                ORDER BY 
                    click_through_rate DESC NULLS LAST,
                    emails_sent DESC  
                LIMIT %s
            """
        
        params.append(limit)
        
        print("Query:", query)
        cursor.execute(query, params)
        results = cursor.fetchall()
        print("Results:", results)

        if not results:
            return "No campaigns found."

        metric_display = metric.replace("_", " ").title()
        
        # Handle singular vs plural output based on limit
        if limit == 1:
            output = f"Top Performing Campaign by {metric_display}:\n\n"
        else:
            output = f"Top {len(results)} Campaigns by {metric_display}:\n\n"

        for i, row in enumerate(results, 1):
            # Use the correct metric value based on the metric selected
            if metric == "conversion_rate":
                metric_val = row['conversion_rate_pct']
            elif metric == "open_rate":
                metric_val = row['open_rate_pct']
            elif metric == "click_through_rate":
                metric_val = row['click_rate_pct']
            else:
                metric_val = row.get('metric_value', 0)
            
            output += (
                f"{i}. {row['subject_line']}\n"
                f"   Date: {row['date']}\n"
                f"   {metric_display}: {metric_val or 0}%\n"
                f"   Emails Sent: {row['emails_sent']:,}\n"
                f"   Conversions: {row['conversions']}\n"
                f"   Open Rate: {row['open_rate_pct']}%\n"
                f"   Click Rate: {row['click_rate_pct']}%\n"
                + "-" * 50 + "\n"
            )

        return output

    except Exception as e:
        return f"Error retrieving top campaigns: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)




@tool
def get_campaign_summary(
    campaign_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> str:
    """
    Generate aggregated statistics and KPIs for email marketing performance.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        query = f"""
            SELECT 
                COUNT(*) as total_campaigns,
                SUM(emails_sent) as total_emails_sent,
                ROUND(AVG(open_rate) * 100, 2) as avg_open_rate,
                ROUND(AVG(click_through_rate) * 100, 2) as avg_click_rate,
                SUM(conversions) as total_conversions,
                SUM(unsubscribes) as total_unsubscribes,
                ROUND(
                    CAST(SUM(conversions) * 100.0 / NULLIF(SUM(emails_sent), 0) AS NUMERIC), 
                    2
                ) as overall_conversion_rate,
                ROUND(
                    CAST(SUM(unsubscribes) * 100.0 / NULLIF(SUM(emails_sent), 0) AS NUMERIC), 
                    2
                ) as unsubscribe_rate,
                MIN(date) as earliest_campaign,
                MAX(date) as latest_campaign
            FROM {TABLE_NAME}
        """

        where_conditions = []
        params = []
        
        if campaign_id:
            where_conditions.append("campaign_id = %s")
            params.append(campaign_id)
        if start_date:
            where_conditions.append("date >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date <= %s")
            params.append(end_date)

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        cursor.execute(query, params)
        result = cursor.fetchone()

        if not result or result["total_campaigns"] == 0:
            return "No campaigns found for the specified criteria."

        output = (
            "Campaign Performance Summary\n"
            + "=" * 40 + "\n\n"
            f"Period: {result['earliest_campaign']} to {result['latest_campaign']}\n"
            f"Total Campaigns: {result['total_campaigns']:,}\n"
            f"Total Emails Sent: {result['total_emails_sent']:,}\n"
            f"Average Open Rate: {result['avg_open_rate']}%\n"
            f"Average Click Rate: {result['avg_click_rate']}%\n"
            f"Total Conversions: {result['total_conversions']:,}\n"
            f"Overall Conversion Rate: {result['overall_conversion_rate'] or 0}%\n"
            f"Total Unsubscribes: {result['total_unsubscribes']:,}\n"
            f"Unsubscribe Rate: {result['unsubscribe_rate'] or 0}%\n"
        )

        return output

    except Exception as e:
        return f"Error generating summary: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)


@tool
def get_campaign_trends(days_to_look_for:Optional[int]=30,campaign_id: Optional[str] = None,) -> str:
    """
    Analyze campaign performance trends over the last 30 days.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        query = f"""
            SELECT 
                campaign_id,
                date as campaign_date,
                COUNT(*) as daily_campaigns,
                ROUND(AVG(open_rate) * 100, 2) as avg_open_rate,
                ROUND(AVG(click_through_rate) * 100, 2) as avg_click_rate,
                SUM(conversions) as daily_conversions,
                SUM(emails_sent) as daily_emails_sent,
                ROUND(
                    CAST(SUM(conversions) * 100.0 / NULLIF(SUM(emails_sent), 0) AS NUMERIC), 
                    2
                ) as daily_conversion_rate
            FROM {TABLE_NAME}
            WHERE date >= CURRENT_DATE - INTERVAL '{days_to_look_for} DAYS'
            GROUP BY date,campaign_id
            ORDER BY date DESC
            LIMIT {days_to_look_for}
        """
        print(query)
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            return "No recent campaign data available."

        output = "Campaign Trends (Last 30 Days)\n" + "=" * 40 + "\n\n"

        for row in results:
            output += (
                f"{row['campaign_date']}: "
                f"{row['daily_campaigns']} campaigns, "
                f"{row['daily_emails_sent']:,} emails sent, "
                f"{row['avg_open_rate']}% open, "
                f"{row['avg_click_rate']}% click, "
                f"{row['daily_conversions']} conversions ({row['daily_conversion_rate'] or 0}%)\n"
            )

        return output

    except Exception as e:
        return f"Error retrieving trends: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)
@tool
def search_campaigns_by_subject(subject_keyword: str, limit:Optional[str] = None) -> str:
    """
    Search email campaigns by subject keyword.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        query = f"""
            SELECT date, campaign_id, subject_line, emails_sent,
                   ROUND(open_rate * 100, 2) as open_rate_pct,
                   ROUND(click_through_rate * 100, 2) as click_rate_pct,
                   conversions, unsubscribes, source,
                   ROUND(
                       CAST(conversions * 100.0 / NULLIF(emails_sent, 0) AS NUMERIC), 
                       2
                   ) as conversion_rate_pct
            FROM {TABLE_NAME}
            WHERE subject_line ILIKE %s
            ORDER BY date DESC
            LIMIT %s
        """

        cursor.execute(query, (f"%{subject_keyword}%", limit))
        results = cursor.fetchall()

        if not results:
            return f"No campaigns found with subject containing '{subject_keyword}'"

        output = f"Found {len(results)} campaigns with '{subject_keyword}' in subject:\n\n"
        for row in results:
            output += (
                f"{row['date']} - {row['subject_line']}\n"
                f"  Campaign ID: {row['campaign_id']}\n"
                f"  Emails Sent: {row['emails_sent']:,}\n"
                f"  Open Rate: {row['open_rate_pct']}% | Click Rate: {row['click_rate_pct']}%\n"
                f"  Conversions: {row['conversions']} ({row['conversion_rate_pct'] or 0}%)\n"
                f"  Unsubscribes: {row['unsubscribes']} | Source: {row['source']}\n\n"
            )

        return output

    except Exception as e:
        return f"Error searching campaigns: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)


@tool(name="Email_marketing_agent",description="This tool is used to give responses related to Email ads related prompts")
def Email_marketing_agent(prompt: str) -> str:
    """
    Intelligent email marketing analysis agent that orchestrates multiple tools.
    """
    agent = Agent(tools=[
        get_campaign_trends,
        get_campaign_summary,
        get_campaign_performance,
        search_campaigns_by_subject
    ])
    response = agent(prompt)


def cleanup_connections():
    """Clean up connection pool"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()

