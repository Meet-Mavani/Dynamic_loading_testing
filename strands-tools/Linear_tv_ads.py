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

TABLE_NAME = "linear_tv_ads"  

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

def combine_results_as_dict(results, summary):
    """Combine results and summary into a structured dictionary"""
    return {
        "summary": summary,
        "results": results,
        "total_count": len(results) if results else 0,
        "has_data": bool(results)
    }

# -------------------------------------------------------------------
# Merged TV network Analysis Tools
# -------------------------------------------------------------------

@tool(name='linear_tv_analyze_tv_network', description=(
        "Retrieve detailed linear TV advertising performance at the network and program level. "
        "This tool allows filtering by date range, network name, program keyword, impressions, reach, and conversion rate. "
        "It returns granular performance metrics per record, including:"
        "Results can be sorted by impressions, reach, frequency, conversions, conversion rate, or date, "
        "with a configurable limit for top results. "
        "When requested, it also provides an aggregated summary across all matching records"
        "Use this tool when users want a detailed breakdown of network/program performance, "
        "ranked lists of top-performing networks/programs, or an overall summary of activity "
        "within a specified time range. Always include the key insights from the retrieved result"
    )
    )
def linear_tv_analyze_tv_network(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    network: Optional[str] = None,
    program_keyword: Optional[str] = None,
    min_impressions: Optional[int] = None,
    min_reach: Optional[int] = None,
    min_conversion_rate: Optional[float] = None,
    sort_by: Optional[str] = "date",
    sort_order: Optional[str] = "desc",
    limit: Optional[int] = 10,
    include_summary: Optional[bool] = False
) -> str:
    """
    Comprehensive TV network analysis tool that retrieves, filters, and sorts network data.
    Can also include summary statistics when requested.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Build main query

        
        query = f"""
            SELECT date, network, program, impressions, reach, frequency, conversions, source,
                   ROUND(
                       CAST(conversions * 100.0 / NULLIF(impressions, 0) AS NUMERIC), 
                       4
                   ) as conversion_rate_pct,
                   ROUND(
                       CAST(impressions * 1.0 / NULLIF(reach, 0) AS NUMERIC), 
                       2
                   ) as calculated_frequency
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
        if network:
            where_conditions.append("UPPER(network) = UPPER(%s)")
            params.append(network)
        if program_keyword:
            where_conditions.append("program ILIKE %s")
            params.append(f"%{program_keyword}%")
        if min_impressions:
            where_conditions.append("impressions >= %s")
            params.append(min_impressions)
        if min_reach:
            where_conditions.append("reach >= %s")
            params.append(min_reach)
        if min_conversion_rate:
            where_conditions.append("(conversions * 100.0 / NULLIF(impressions, 0)) >= %s")
            params.append(min_conversion_rate)

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        # Add sorting
        sort_mapping = {
            "date": "date",
            "impressions": "impressions",
            "reach": "reach",
            "frequency": "frequency",
            "conversions": "conversions",
            "conversion_rate": "conversions * 100.0 / NULLIF(impressions, 0)"
        }
        
        sort_field = sort_mapping.get(sort_by, "date")
        order = "ASC" if sort_order.lower() == "asc" else "DESC"
        query += f" ORDER BY {sort_field} {order} LIMIT %s"
        params.append(limit)
        
        print("Query:", query)
        print("Params:", params)
        
        # Execute main query
        cursor.execute(query, params)
        results = cursor.fetchall()

        if not results:
            return "No TV networks found matching the criteria."
        

        # Initialize output
        output = ""
        summary = None
        
        # Get summary if requested
        if include_summary:
            summary_query = f"""
                SELECT 
                    COUNT(*) as total_networks,
                    COUNT(DISTINCT network) as unique_networks,
                    SUM(impressions) as total_impressions,
                    SUM(reach) as total_reach,
                    ROUND(AVG(frequency), 2) as avg_frequency,
                    SUM(conversions) as total_conversions,
                    ROUND(
                        CAST(SUM(conversions) * 100.0 / NULLIF(SUM(impressions), 0) AS NUMERIC), 
                        4
                    ) as overall_conversion_rate,
                    MIN(date) as earliest_network,
                    MAX(date) as latest_network
                FROM {TABLE_NAME}
            """
            
            summary_params = []
            if where_conditions:
                summary_query += " WHERE " + " AND ".join(where_conditions)
                summary_params = params[:-1]  # Exclude limit param
            
            cursor.execute(summary_query, summary_params)
            summary = cursor.fetchone()
            print("Summary result:", summary)
            
            # Add summary to output
            if summary:
                return results,summary
            else:
                return results
                # output += (
                #     "NETWORK SUMMARY\n"
                #     + "=" * 40 + "\n"
                #     f"Period: {summary['earliest_network']} to {summary['latest_network']}\n"
                #     f"Total Records: {summary['total_networks']:,}\n"
                #     f"Unique Networks: {summary['unique_networks']}\n"
                #     f"Total Impressions: {summary['total_impressions']:,}\n"
                #     f"Total Reach: {summary['total_reach']:,}\n"
                #     f"Average Frequency: {summary['avg_frequency']}\n"
                #     f"Total Conversions: {summary['total_conversions']:,}\n"
                #     f"Overall Conversion Rate: {summary['overall_conversion_rate'] or 0}%\n\n"
                # )

        # Add detailed results to output
        # output += f"DETAILED RESULTS ({len(results)} records):\n" + "=" * 50 + "\n\n"
        
        # for i, row in enumerate(results, 1):
        #     output += (
        #         f"{i}. Date: {row['date']} | Network: {row['network']}\n"
        #         f"   Program: {row['program']}\n"
        #         f"   Impressions: {row['impressions']:,} | Reach: {row['reach']:,}\n"
        #         f"   Frequency: {row['frequency']} | Conversions: {row['conversions']}\n"
        #         f"   Conversion Rate: {row['conversion_rate_pct'] or 0}%\n"
        #         f"   Source: {row['source']}\n"
        #         + "-" * 50 + "\n"
        #     )
        # print("output:::::::::",output)
        # return output

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return f"Error analyzing TV networks: {str(e)}"
    finally:
        if connection:
            cursor.close()
            return_connection(connection)

@tool(
    name="linear_tv_get_network_trends", 
    description="""
    Analyze network_and_program performance trends over time across different networks and programs.
    Supports daily, weekly, or monthly aggregation for impressions, reach, frequency, conversions.
    Shows trend direction and percentage changes over time for multi-network network_and_program analysis.
    Use this when users ask about performance trends, network comparisons, seasonal patterns, or time-based analysis.
    """
)
def linear_tv_get_network_and_program_trends(
    network: Optional[str] = None,
    program: Optional[str] = None,
    source: Optional[str] = None,
    period: Literal['daily', 'weekly', 'monthly'] = 'daily',
    metric: Literal['impressions', 'reach', 'frequency', 'conversions'] = 'impressions',
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    days_back: Optional[int] = 30
) -> str:
    """
    Analyze trends in network_and_program performance over time across networks and programs
    
    Args:
        network: Filter by specific network (e.g., 'Facebook', 'Google', 'LinkedIn')
        program: Filter by specific program/network_and_program name
        source: Filter by traffic source
        period: Aggregation period (daily, weekly, monthly)
        metric: Metric to analyze trends for (impressions, reach, frequency, conversions)
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
        
        if network:
            where_conditions.append("LOWER(network) LIKE LOWER(%s)")
            params.append(f"%{network}%")
        
        if program:
            where_conditions.append("LOWER(program) LIKE LOWER(%s)")
            params.append(f"%{program}%")
        
        if source:
            where_conditions.append("LOWER(source) LIKE LOWER(%s)")
            params.append(f"%{source}%")
        
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
            SUM(impressions) as total_impressions,
            SUM(reach) as total_reach,
            ROUND(AVG(frequency), 3) as avg_frequency,
            SUM(conversions) as total_conversions,
            ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 4) as conversion_rate,
            ROUND((SUM(reach)::DECIMAL / NULLIF(SUM(impressions), 0)), 4) as reach_rate,
            COUNT(DISTINCT network) as network_count,
            COUNT(DISTINCT program) as program_count,
            COUNT(DISTINCT source) as source_count,
            COUNT(*) as record_count
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
        
        # Build filter description
        filter_desc = []
        if network:
            filter_desc.append(f"Network: {network}")
        if program:
            filter_desc.append(f"Program: {program}")
        if source:
            filter_desc.append(f"Source: {source}")
        
        filter_text = f" (Filtered by: {', '.join(filter_desc)})" if filter_desc else ""
        
        output = f"network_and_program Trends Analysis - {period.title()} View{filter_text}\n" + "=" * 80 + "\n\n"
        
        # Calculate trend direction and percentage change
        if len(trend_results) > 1:
            metric_key = f"total_{metric}" if metric in ['impressions', 'reach', 'conversions'] else f"avg_{metric}"
            
            first_value = float(trend_results[0].get(metric_key, 0))
            last_value = float(trend_results[-1].get(metric_key, 0))
            
            if first_value > 0:
                percent_change = ((last_value - first_value) / first_value) * 100
                trend_direction = "increasing" if last_value > first_value else "decreasing"
                output += f" Trend Analysis for {metric.upper()}:\n"
                output += f"   Direction: {trend_direction.title()} ({percent_change:+.1f}%)\n"
                output += f"   First Period: {first_value:,.0f}\n"
                output += f"   Last Period: {last_value:,.0f}\n"
                output += f"   Periods Analyzed: {len(trend_results)}\n\n"
        
        # Summary statistics
        total_impressions = sum(row['total_impressions'] for row in trend_results)
        total_reach = sum(row['total_reach'] for row in trend_results)
        total_conversions = sum(row['total_conversions'] for row in trend_results)
        avg_frequency = sum(row['avg_frequency'] for row in trend_results) / len(trend_results)
        
        output += f"Summary Statistics:\n"
        output += f"   Total Impressions: {total_impressions:,}\n"
        output += f"   Total Reach: {total_reach:,}\n"
        output += f"   Total Conversions: {total_conversions:,}\n"
        output += f"   Average Frequency: {avg_frequency:.3f}\n"
        if total_impressions > 0:
            output += f"   Overall Conversion Rate: {(total_conversions / total_impressions * 100):.4f}%\n"
        output += f"   Unique Networks: {max(row['network_count'] for row in trend_results)}\n"
        output += f"   Unique Programs: {max(row['program_count'] for row in trend_results)}\n"
        output += f"   Unique Sources: {max(row['source_count'] for row in trend_results)}\n\n"
        
        # Detailed period breakdown
        output += f"Detailed {period.title()} Breakdown:\n"
        output += "-" * 120 + "\n"
        output += f"{'Period':<12} {'Impressions':<12} {'Reach':<10} {'Frequency':<10} {'Conversions':<12} {'Conv.Rate':<10} {'Reach Rate':<11} {'Networks':<9} {'Programs':<9}\n"
        output += "-" * 120 + "\n"
        
        for row in trend_results:
            conv_rate = (row['total_conversions'] / row['total_impressions'] * 100) if row['total_impressions'] > 0 else 0
            reach_rate = (row['total_reach'] / row['total_impressions']) if row['total_impressions'] > 0 else 0
            
            output += (
                f"{row['period']:<12} "
                f"{row['total_impressions']:>11,} "
                f"{row['total_reach']:>9,} "
                f"{row['avg_frequency']:>9.3f} "
                f"{row['total_conversions']:>11,} "
                f"{conv_rate:>9.2f}% "
                f"{reach_rate:>10.4f} "
                f"{row['network_count']:>8} "
                f"{row['program_count']:>8}\n"
            )
        
        return output
        
    except Exception as e:
        return f" Error processing trends: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)


# Additional helper function for network-specific analysis
@tool(
    name="linear_tv_get_network_comparison",
    description="""
    Compare performance metrics across different networks over a specified time period.
    Provides side-by-side comparison of impressions, reach, frequency, conversions by network.
    Use when users want to compare network performance or identify top-performing networks.
    """
)
def linear_tv_get_network_comparison(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    compare_network_1:Optional[str] = None,
    compare_network_2:Optional[str] = None,
    days_back: Optional[int] = 30,
    metric: Literal['impressions', 'reach', 'frequency', 'conversions'] = 'impressions',
    top_n: Optional[int] = 10
) -> str:
    """
    Compare network_and_program performance across different networks
    
    Args:
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        compare_network_1: Network 1 of the comparison
        compare_network_2: Network 2 of the Comparison
        days_back: Number of days to look back if no date range specified
        metric: Primary metric to rank networks by
        top_n: Limit to top N networks by the specified metric
    """
    connection = None
    cursor = None
    
    try:
        # Build WHERE conditions for date filtering
        where_conditions = ["impressions > 0 AND reach > 0"]
        params = []
        
        if compare_network_1 and compare_network_2: 
            where_conditions.append("network ILIKE LOWER(%s) OR network ILIKE LOWER(%s)")
            params.append(f"%{compare_network_1}%")
            params.append(f"%{compare_network_2}%")


        if date_from:
            where_conditions.append("date >= %s")
            params.append(date_from)
        elif not date_to:
            where_conditions.append(f"date >= CURRENT_DATE - INTERVAL '{days_back} days'")
        
        if date_to:
            where_conditions.append("date <= %s")
            params.append(date_to)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Determine ORDER BY clause based on metric
        order_metric = f"total_{metric}" if metric in ['impressions', 'reach', 'conversions'] else f"avg_{metric}"

        query = f"""
        SELECT 
            network,
            SUM(impressions) AS total_impressions,
            SUM(reach) AS total_reach,
            ROUND(AVG(frequency), 2) AS avg_frequency,
            SUM(conversions) AS total_conversions,
            ROUND(SUM(conversions) * 100.0 / NULLIF(SUM(impressions), 0), 4) AS conversion_rate_pct
        FROM {TABLE_NAME} 
        {where_clause}
        GROUP BY network
        ORDER BY {order_metric} DESC
        {f'LIMIT {top_n}' if top_n else ''}
        """
        
        print(f"Executing network comparison query: {query}")
        print(f"Parameters: {params}")
        
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        network_results = cursor.fetchall()
        
        if not network_results:
            return "No network data found for the specified criteria."
        else:
            return network_results
        
        # output = f"Network Performance Comparison (Ranked by {metric.title()})\n" + "=" * 90 + "\n\n"
        
        # # Summary totals
        # total_impressions = sum(row['total_impressions'] for row in network_results)
        # total_reach = sum(row['total_reach'] for row in network_results)
        # total_conversions = sum(row['total_conversions'] for row in network_results)
        
        # output += f"Overall Totals: {total_impressions:,} impressions, {total_reach:,} reach, {total_conversions:,} conversions\n\n"
        
        # # Network breakdown
        # output += f"{'Network':<15} {'Impressions':<12} {'Reach':<10} {'Frequency':<10} {'Conversions':<12} {'Conv.Rate':<10} {'Programs':<9} {'Sources':<8}\n"
        # output += "-" * 90 + "\n"
        
        # for i, row in enumerate(network_results, 1):
        #     output += (
        #         f"{row['network']:<15} "
        #         f"{row['total_impressions']:>11,} "
        #         f"{row['total_reach']:>9,} "
        #         f"{row['avg_frequency']:>9.3f} "
        #         f"{row['total_conversions']:>11,} "
        #         f"{row['conversion_rate']:>9.2f}% "
        #         f"{row['program_count']:>8} "
        #         f"{row['source_count']:>7}\n"
        #     )
        
        # return output
        
    except Exception as e:
        return f"Error processing network comparison: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)

@tool(name="Linear_TV_advertising_agent", 
     description=(
        "An intelligent Linear TV advertising analytics agent that interprets natural language queries "
        "and routes them to specialized sub-tools for detailed performance analysis. "
        "It provides insights at the network, program, and trend levels to help understand the impact of TV ad placements."
        "Capabilities include:\n"
        "- **Network & program performance analysis**: Impressions, reach, frequency, conversions, conversion rate, and placement source at the network or program level.\n"
        "- **Trend analysis**: Daily, weekly, or monthly trends of impressions, reach, frequency, and conversions to track performance changes over time.\n"
        "- **Network & program comparison**: Side-by-side comparison of multiple networks or programs across impressions, reach, frequency, conversions, and efficiency metrics.\n\n"
        "Use this tool whenever users ask broad questions about Linear TV advertising performance, "
        "want to track trends, or compare different networks/programs without specifying the exact sub-tool. "
        "The agent ensures all responses are returned in **markdown format** with key insights clearly highlighted."
    )

)
def Linear_TV_advertising_agent(prompt: str) -> str:
    """
    Intelligent TV advertising analysis agent that orchestrates the merged analysis tools.
    """
    agent = Agent(system_prompt="You are a helpful tv advertising agent who always return the the response in detailed manner with the mandatory key insights from the retrieved data",tools=[
        linear_tv_analyze_tv_network,
        linear_tv_get_network_comparison,
        linear_tv_get_network_and_program_trends
    ])
    response = agent(prompt)
    return response


def cleanup_connections():
    """Clean up connection pool"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()


# TV_advertising_agent("Give me weekly trends for the CBS network in the first quarter in 2023 also include the per month details")