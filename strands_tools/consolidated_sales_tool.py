from typing import Optional, Literal, List
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

TABLE_NAME = "consolidated_profit"  # Update with your actual table name

# Global connection pool
connection_pool = None

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

def customer_behavior_agent(prompt: str) -> str:
    """Main customer behavior analysis agent"""
    supervisor_agent = Agent(
        tools=[analyze_customer_purchase_behavior]
    )
    
    response = supervisor_agent(prompt)
    return str(response)

@tool(
    name="analyze_customer_purchase_behavior", 
    description="""
    Comprehensive customer purchase behavior analysis tool that leverages unique customer IDs and profit data.
    Analyzes customer profitability, purchase patterns, category preferences, merchant relationships, and payment behaviors.
    Supports filtering by customer ID, category, merchant, profitability thresholds, and time periods.
    Provides insights into customer lifetime value, average transaction values, preferred categories, and payment patterns.
    Use this when you need to understand customer behavior, identify high-value customers, analyze purchase patterns, 
    or get insights into customer profitability and preferences.
    """
)
def analyze_customer_purchase_behavior(
    customer_id: Optional[str] = None,
    category: Optional[str] = None,
    merchant_name: Optional[str] = None,
    analysis_type: Literal['customer_profile', 'profitability_analysis', 'category_preferences', 'merchant_relationships', 'payment_behavior', 'lifetime_value'] = 'customer_profile',
    min_gross_profit: Optional[float] = None,
    min_net_profit: Optional[float] = None,
    min_transactions: Optional[int] = 1,
    sort_by: Literal['total_gross_profit', 'total_net_profit', 'transaction_count', 'avg_payment', 'customer_id'] = 'total_gross_profit',
    limit: Optional[int] = 20
) -> str:
    """
    Analyze customer purchase behavior using Customer ID and profit data
    
    Args:
        customer_id: Analyze specific customer (exact match)
        category: Filter by product/service category
        merchant_name: Filter by specific merchant
        analysis_type: Type of analysis to perform
        min_gross_profit: Minimum total gross profit threshold
        min_net_profit: Minimum total net profit threshold  
        min_transactions: Minimum number of transactions required
        sort_by: How to sort the results
        limit: Maximum number of customers to analyze
    """
    connection = None
    cursor = None
    
    try:
        # Build WHERE conditions
        where_conditions = []
        params = []
        
        if customer_id:
            where_conditions.append("\"Customer ID\" = %s")
            params.append(customer_id)
        
        if category:
            where_conditions.append("UPPER(\"Category\") = UPPER(%s)")
            params.append(category)
            
        if merchant_name:
            where_conditions.append("UPPER(\"Merchant_Name\") = UPPER(%s)")
            params.append(merchant_name)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build HAVING conditions for aggregated metrics
        having_conditions = []
        if min_gross_profit:
            having_conditions.append("SUM(\"Gross Profit\") >= %s")
            params.append(min_gross_profit)
            
        if min_net_profit:
            having_conditions.append("SUM(\"Net Profit\") >= %s")
            params.append(min_net_profit)
            
        if min_transactions:
            having_conditions.append("COUNT(*) >= %s")
            params.append(min_transactions)
        
        having_clause = "HAVING " + " AND ".join(having_conditions) if having_conditions else ""
        
        # Base query for customer behavior analysis
        if analysis_type == 'customer_profile':
            query = f"""
            SELECT 
                "Customer ID",
                COUNT(*) as transaction_count,
                SUM("Gross Profit") as total_gross_profit,
                SUM("Net Profit") as total_net_profit,
                SUM("Customer Payment") as total_payments,
                AVG("Gross Profit") as avg_gross_profit,
                AVG("Net Profit") as avg_net_profit,
                AVG("Customer Payment") as avg_payment,
                ROUND((SUM("Net Profit") / NULLIF(SUM("Gross Profit"), 0)) * 100, 2) as profit_margin_pct,
                COUNT(DISTINCT "Category") as categories_purchased,
                COUNT(DISTINCT "Merchant_Name") as merchants_used,
                STRING_AGG(DISTINCT "Category", ', ') as preferred_categories,
                STRING_AGG(DISTINCT "Merchant_Name", ', ') as used_merchants,
                MIN("Gross Profit") as min_transaction_gross,
                MAX("Gross Profit") as max_transaction_gross
            FROM {TABLE_NAME}
            {where_clause}
            GROUP BY "Customer ID"
            {having_clause}
            ORDER BY {sort_by} DESC
            LIMIT %s
            """
            
        elif analysis_type == 'category_preferences':
            query = f"""
            SELECT 
                "Customer ID",
                "Category",
                COUNT(*) as transactions_in_category,
                SUM("Gross Profit") as category_gross_profit,
                SUM("Net Profit") as category_net_profit,
                SUM("Customer Payment") as category_payments,
                AVG("Gross Profit") as avg_category_gross_profit,
                ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY "Customer ID")), 2) as category_percentage
            FROM {TABLE_NAME}
            {where_clause}
            GROUP BY "Customer ID", "Category"
            {having_clause}
            ORDER BY "Customer ID", category_gross_profit DESC
            LIMIT %s
            """
            
        elif analysis_type == 'merchant_relationships':
            query = f"""
            SELECT 
                "Customer ID",
                "Merchant_Name",
                COUNT(*) as transactions_with_merchant,
                SUM("Gross Profit") as merchant_gross_profit,
                SUM("Net Profit") as merchant_net_profit,
                SUM("Customer Payment") as merchant_payments,
                AVG("Gross Profit") as avg_merchant_gross_profit,
                ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY "Customer ID")), 2) as merchant_percentage
            FROM {TABLE_NAME}
            {where_clause}
            GROUP BY "Customer ID", "Merchant_Name"
            {having_clause}
            ORDER BY "Customer ID", merchant_gross_profit DESC
            LIMIT %s
            """
            
        elif analysis_type == 'payment_behavior':
            query = f"""
            SELECT 
                "Customer ID",
                COUNT(*) as total_transactions,
                SUM("Customer Payment") as total_payments,
                SUM("Gross Profit") as total_gross_profit,
                AVG("Customer Payment") as avg_payment_amount,
                COUNT(CASE WHEN "Customer Payment" = 0 THEN 1 END) as zero_payment_count,
                COUNT(CASE WHEN "Customer Payment" > 0 THEN 1 END) as paid_transaction_count,
                ROUND((COUNT(CASE WHEN "Customer Payment" = 0 THEN 1 END) * 100.0 / COUNT(*)), 2) as zero_payment_percentage,
                ROUND((SUM("Customer Payment") / NULLIF(SUM("Gross Profit"), 0)) * 100, 2) as payment_to_profit_ratio
            FROM {TABLE_NAME}
            {where_clause}
            GROUP BY "Customer ID"
            {having_clause}
            ORDER BY {sort_by} DESC
            LIMIT %s
            """
            
        elif analysis_type == 'lifetime_value':
            query = f"""
            SELECT 
                "Customer ID",
                COUNT(*) as lifetime_transactions,
                SUM("Gross Profit") as lifetime_gross_profit,
                SUM("Net Profit") as lifetime_net_profit,
                SUM("Customer Payment") as lifetime_payments,
                AVG("Gross Profit") as avg_transaction_value,
                ROUND(SUM("Net Profit") / COUNT(*), 2) as avg_profit_per_transaction,
                COUNT(DISTINCT "Category") as category_diversity,
                COUNT(DISTINCT "Merchant_Name") as merchant_diversity,
                ROUND((SUM("Net Profit") / NULLIF(SUM("Gross Profit"), 0)) * 100, 2) as overall_margin_pct
            FROM {TABLE_NAME}
            {where_clause}
            GROUP BY "Customer ID"
            {having_clause}
            ORDER BY {sort_by} DESC
            LIMIT %s
            """
            
        else:  # profitability_analysis
            query = f"""
            SELECT 
                "Customer ID",
                COUNT(*) as transaction_count,
                SUM("Gross Profit") as total_gross_profit,
                SUM("Net Profit") as total_net_profit,
                SUM("Gross Profit") - SUM("Net Profit") as total_costs,
                ROUND((SUM("Net Profit") / NULLIF(SUM("Gross Profit"), 0)) * 100, 2) as profit_margin,
                ROUND(SUM("Net Profit") / COUNT(*), 2) as avg_profit_per_transaction,
                MAX("Net Profit") as highest_profit_transaction,
                MIN("Net Profit") as lowest_profit_transaction
            FROM {TABLE_NAME}
            {where_clause}
            GROUP BY "Customer ID"
            {having_clause}
            ORDER BY {sort_by} DESC
            LIMIT %s
            """
        
        params.append(limit)
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        if not results:
            return f"No customer data found matching the specified criteria."
        
        # Format output based on analysis type
        output = f"Customer Purchase Behavior Analysis - {analysis_type.replace('_', ' ').title()}\n" + "=" * 80 + "\n\n"
        
        if analysis_type == 'customer_profile':
            for row in results:
                output += (
                    f"Customer ID: {row['Customer ID']}\n"
                    f"Total Transactions: {row['transaction_count']}\n"
                    f"Total Gross Profit: ${row['total_gross_profit']:,.2f}\n"
                    f"Total Net Profit: ${row['total_net_profit']:,.2f}\n"
                    f"Total Payments: ${row['total_payments']:,.2f}\n"
                    f"Average Transaction (Gross): ${row['avg_gross_profit']:,.2f}\n"
                    f"Average Transaction (Net): ${row['avg_net_profit']:,.2f}\n"
                    f"Average Payment: ${row['avg_payment']:,.2f}\n"
                    f"Profit Margin: {row['profit_margin_pct']}%\n"
                    f"Categories Purchased: {row['categories_purchased']}\n"
                    f"Merchants Used: {row['merchants_used']}\n"
                    f"Preferred Categories: {row['preferred_categories']}\n"
                    f"Transaction Range: ${row['min_transaction_gross']:,.2f} - ${row['max_transaction_gross']:,.2f}\n"
                    + "-" * 80 + "\n"
                )
                
        elif analysis_type == 'payment_behavior':
            for row in results:
                output += (
                    f"Customer ID: {row['Customer ID']}\n"
                    f"Total Transactions: {row['total_transactions']}\n"
                    f"Total Payments Made: ${row['total_payments']:,.2f}\n"
                    f"Average Payment: ${row['avg_payment_amount']:,.2f}\n"
                    f"Zero Payment Transactions: {row['zero_payment_count']}\n"
                    f"Paid Transactions: {row['paid_transaction_count']}\n"
                    f"Zero Payment Rate: {row['zero_payment_percentage']}%\n"
                    f"Payment to Profit Ratio: {row['payment_to_profit_ratio']}%\n"
                    + "-" * 80 + "\n"
                )
        else:
            # Generic formatting for other analysis types
            for row in results:
                output += f"Customer ID: {row['Customer ID']}\n"
                for key, value in row.items():
                    if key != 'Customer ID':
                        if isinstance(value, (int, float)) and 'profit' in key.lower():
                            output += f"{key.replace('_', ' ').title()}: ${value:,.2f}\n"
                        elif isinstance(value, float):
                            output += f"{key.replace('_', ' ').title()}: {value:,.2f}\n"
                        else:
                            output += f"{key.replace('_', ' ').title()}: {value}\n"
                output += "-" * 80 + "\n"
        
        return output
        
    except Exception as e:
        return f"Error analyzing customer behavior: {str(e)}"
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            return_connection(connection)