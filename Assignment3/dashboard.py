import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Business Intelligence Dashboard - Sales Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            port=int(os.getenv("PGPORT")),
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()

# Custom CSS for better styling
def local_css():
    st.markdown("""
        <style>
        .main-header {
            font-size: 3rem;
            font-weight: 700;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stMetric {
            background-color: #ffffff;
            padding: 1rem;
            border-radius: 0.5rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        </style>
    """, unsafe_allow_html=True)

# Data loading functions
@st.cache_data(ttl=600)
def load_sales_data():
    """Load sales data with all dimensions joined"""
    conn = get_connection()
    query = """
    SELECT 
        f.order_number,
        f.order_line_number,
        f.quantity_ordered,
        f.price_each,
        f.sales,
        f.deal_size,
        p.product_code,
        p.product_line,
        p.msrp,
        c.customer_name,
        c.city,
        c.state,
        c.country,
        c.territory,
        d.order_date,
        d.year_id,
        d.month_id,
        d.month_name,
        d.qtr_id,
        d.day_of_week
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_date d ON f.date_key = d.date_key
    ORDER BY d.order_date
    """
    df = pd.read_sql(query, conn)
    df['order_date'] = pd.to_datetime(df['order_date'])
    return df

@st.cache_data(ttl=600)
def get_kpi_metrics(df):
    """Calculate KPI metrics"""
    total_revenue = df['sales'].sum()
    total_orders = df['order_number'].nunique()
    total_quantity = df['quantity_ordered'].sum()
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    unique_customers = df['customer_name'].nunique()
    unique_products = df['product_code'].nunique()
    
    return {
        'total_revenue': total_revenue,
        'total_orders': total_orders,
        'total_quantity': total_quantity,
        'avg_order_value': avg_order_value,
        'unique_customers': unique_customers,
        'unique_products': unique_products
    }

@st.cache_data(ttl=600)
def get_year_over_year_growth(df):
    """Calculate year-over-year growth metrics"""
    yearly_sales = df.groupby('year_id')['sales'].sum().sort_index()
    if len(yearly_sales) > 1:
        current_year = yearly_sales.iloc[-1]
        previous_year = yearly_sales.iloc[-2]
        yoy_growth = ((current_year - previous_year) / previous_year) * 100
        return yoy_growth
    return 0

# Main Dashboard
def main():
    # Apply custom CSS
    local_css()
    
    # Title with custom styling
    st.markdown('<h1 class="main-header">Business Intelligence Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; font-size: 1.6rem; color: #FFF;">Sales Analytics & Performance Insights</p>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data from PostgreSQL..."):
        try:
            df = load_sales_data()
            st.success(f"Successfully loaded {len(df):,} records")
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            st.stop()
    
    # Sidebar filters
    st.sidebar.title("Filter Controls")
    st.sidebar.markdown("---")
    
    # Date range filter
    min_date = df['order_date'].min().date()
    max_date = df['order_date'].max().date()
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Product line filter
    product_lines = ['All'] + sorted(df['product_line'].unique().tolist())
    selected_product = st.sidebar.selectbox("Product Line", product_lines)
    
    # Country filter
    countries = ['All'] + sorted(df['country'].unique().tolist())
    selected_country = st.sidebar.selectbox("Country", countries)
    
    # Deal size filter
    deal_sizes = ['All'] + sorted(df['deal_size'].unique().tolist())
    selected_deal_size = st.sidebar.selectbox("Deal Size", deal_sizes)
    
    # Apply filters
    filtered_df = df.copy()
    
    if len(date_range) == 2:
        filtered_df = filtered_df[
            (filtered_df['order_date'].dt.date >= date_range[0]) &
            (filtered_df['order_date'].dt.date <= date_range[1])
        ]
    
    if selected_product != 'All':
        filtered_df = filtered_df[filtered_df['product_line'] == selected_product]
    
    if selected_country != 'All':
        filtered_df = filtered_df[filtered_df['country'] == selected_country]
    
    if selected_deal_size != 'All':
        filtered_df = filtered_df[filtered_df['deal_size'] == selected_deal_size]
    
    # KPIs Section
    st.header("Key Performance Indicators (KPIs)")
    kpis = get_kpi_metrics(filtered_df)
    yoy_growth = get_year_over_year_growth(filtered_df)
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="Total Revenue",
            value=f"${kpis['total_revenue']:,.0f}",
            delta=f"{yoy_growth:.1f}% YoY" if yoy_growth != 0 else None
        )
    
    with col2:
        st.metric(
            label="Total Orders",
            value=f"{kpis['total_orders']:,}"
        )
    
    with col3:
        st.metric(
            label="Items Sold",
            value=f"{kpis['total_quantity']:,}"
        )
    
    with col4:
        st.metric(
            label="Avg Order Value",
            value=f"${kpis['avg_order_value']:,.2f}"
        )
    
    with col5:
        st.metric(
            label="Customers",
            value=f"{kpis['unique_customers']:,}"
        )
    
    with col6:
        st.metric(
            label="Products",
            value=f"{kpis['unique_products']:,}"
        )
    
    st.markdown("---")
    
    # Charts Row 1
    st.header("Sales Trends")
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue over time
        revenue_by_month = filtered_df.groupby(['year_id', 'month_name', 'month_id'])['sales'].sum().reset_index()
        revenue_by_month = revenue_by_month.sort_values(['year_id', 'month_id'])
        revenue_by_month['period'] = revenue_by_month['year_id'].astype(str) + '-' + revenue_by_month['month_name']
        
        fig_revenue_trend = px.line(
            revenue_by_month,
            x='period',
            y='sales',
            title='Revenue Trend Over Time',
            labels={'sales': 'Revenue ($)', 'period': 'Period'}
        )
        fig_revenue_trend.update_traces(line_color='#1f77b4', line_width=3)
        st.plotly_chart(fig_revenue_trend, use_container_width=True)
    
    with col2:
        # Sales by product line
        sales_by_product = filtered_df.groupby('product_line')['sales'].sum().reset_index()
        sales_by_product = sales_by_product.sort_values('sales', ascending=False)
        
        fig_product = px.bar(
            sales_by_product,
            x='product_line',
            y='sales',
            title='Revenue by Product Line',
            labels={'sales': 'Revenue ($)', 'product_line': 'Product Line'},
            color='sales',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_product, use_container_width=True)
    
    # Charts Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        # Sales by country (top 10)
        sales_by_country = filtered_df.groupby('country')['sales'].sum().reset_index()
        sales_by_country = sales_by_country.sort_values('sales', ascending=False).head(10)
        
        fig_country = px.bar(
            sales_by_country,
            x='sales',
            y='country',
            orientation='h',
            title='Top 10 Countries by Revenue',
            labels={'sales': 'Revenue ($)', 'country': 'Country'},
            color='sales',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig_country, use_container_width=True)
    
    with col2:
        # Deal size distribution
        deal_size_dist = filtered_df.groupby('deal_size')['sales'].sum().reset_index()
        
        fig_deal = px.pie(
            deal_size_dist,
            values='sales',
            names='deal_size',
            title='Revenue Distribution by Deal Size',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        st.plotly_chart(fig_deal, use_container_width=True)
    
    # Charts Row 3
    st.header("Additional Insights")
    
    # Create tabs for different insights
    tab1, tab2, tab3 = st.tabs(["Customer Analysis", "Product Performance", "Time Analysis"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top 10 customers
            top_customers = filtered_df.groupby('customer_name')['sales'].sum().reset_index()
            top_customers = top_customers.sort_values('sales', ascending=False).head(10)
            
            fig_customers = px.bar(
                top_customers,
                x='sales',
                y='customer_name',
                orientation='h',
                title='Top 10 Customers by Revenue',
                labels={'sales': 'Revenue ($)', 'customer_name': 'Customer'},
                color='sales',
                color_continuous_scale='Oranges'
            )
            fig_customers.update_layout(height=500)
            st.plotly_chart(fig_customers, use_container_width=True)
        
        with col2:
            # Customer distribution by country
            customer_country = filtered_df.groupby('country')['customer_name'].nunique().reset_index()
            customer_country.columns = ['country', 'num_customers']
            customer_country = customer_country.sort_values('num_customers', ascending=False).head(10)
            
            fig_cust_country = px.bar(
                customer_country,
                x='country',
                y='num_customers',
                title='Customer Distribution by Country (Top 10)',
                labels={'num_customers': 'Number of Customers', 'country': 'Country'},
                color='num_customers',
                color_continuous_scale='Blues'
            )
            fig_cust_country.update_layout(height=500)
            st.plotly_chart(fig_cust_country, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Product performance by MSRP vs actual sales
            product_perf = filtered_df.groupby('product_code').agg({
                'sales': 'sum',
                'quantity_ordered': 'sum',
                'msrp': 'first',
                'product_line': 'first'
            }).reset_index()
            product_perf = product_perf.sort_values('sales', ascending=False).head(15)
            
            fig_product_perf = px.scatter(
                product_perf,
                x='quantity_ordered',
                y='sales',
                size='msrp',
                color='product_line',
                hover_data=['product_code'],
                title='Product Performance: Quantity vs Revenue (Top 15)',
                labels={'quantity_ordered': 'Quantity Sold', 'sales': 'Revenue ($)'}
            )
            fig_product_perf.update_layout(height=500)
            st.plotly_chart(fig_product_perf, use_container_width=True)
        
        with col2:
            # Average order value by product line
            avg_order_product = filtered_df.groupby('product_line')['price_each'].mean().reset_index()
            avg_order_product = avg_order_product.sort_values('price_each', ascending=False)
            
            fig_avg_price = px.bar(
                avg_order_product,
                x='product_line',
                y='price_each',
                title='Average Price by Product Line',
                labels={'price_each': 'Average Price ($)', 'product_line': 'Product Line'},
                color='price_each',
                color_continuous_scale='Greens'
            )
            fig_avg_price.update_layout(height=500)
            st.plotly_chart(fig_avg_price, use_container_width=True)
    
    with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            # Quarterly performance
            quarterly_sales = filtered_df.groupby(['year_id', 'qtr_id'])['sales'].sum().reset_index()
            quarterly_sales['quarter'] = 'Q' + quarterly_sales['qtr_id'].astype(str) + ' ' + quarterly_sales['year_id'].astype(str)
            quarterly_sales = quarterly_sales.sort_values(['year_id', 'qtr_id'])
            
            fig_quarterly = px.bar(
                quarterly_sales,
                x='quarter',
                y='sales',
                title='Quarterly Sales Performance',
                labels={'sales': 'Revenue ($)', 'quarter': 'Quarter'},
                color='sales',
                color_continuous_scale='Purples'
            )
            fig_quarterly.update_layout(height=500)
            st.plotly_chart(fig_quarterly, use_container_width=True)
        
        with col2:
            # Sales by day of week
            dow_sales = filtered_df.groupby('day_of_week')['sales'].sum().reset_index()
            # Order days properly
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            dow_sales['day_of_week'] = pd.Categorical(dow_sales['day_of_week'], categories=day_order, ordered=True)
            dow_sales = dow_sales.sort_values('day_of_week')
            
            fig_dow = px.bar(
                dow_sales,
                x='day_of_week',
                y='sales',
                title='Sales by Day of Week',
                labels={'sales': 'Revenue ($)', 'day_of_week': 'Day'},
                color='sales',
                color_continuous_scale='Reds'
            )
            fig_dow.update_layout(height=500)
            st.plotly_chart(fig_dow, use_container_width=True)
    
    st.markdown("---")
    
    # Data table
    st.header("Detailed Sales Data")
    
    # Summary statistics
    with st.expander("View Summary Statistics", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Sales Statistics")
            stats_df = pd.DataFrame({
                'Metric': ['Mean Sales', 'Median Sales', 'Std Dev', 'Min Sales', 'Max Sales'],
                'Value': [
                    f"${filtered_df['sales'].mean():,.2f}",
                    f"${filtered_df['sales'].median():,.2f}",
                    f"${filtered_df['sales'].std():,.2f}",
                    f"${filtered_df['sales'].min():,.2f}",
                    f"${filtered_df['sales'].max():,.2f}"
                ]
            })
            st.dataframe(stats_df, hide_index=True, use_container_width=True)
        
        with col2:
            st.subheader("Order Statistics")
            order_stats_df = pd.DataFrame({
                'Metric': ['Avg Quantity/Order', 'Avg Price/Item', 'Total Customers', 'Total Products'],
                'Value': [
                    f"{filtered_df['quantity_ordered'].mean():.2f}",
                    f"${filtered_df['price_each'].mean():,.2f}",
                    f"{filtered_df['customer_name'].nunique():,}",
                    f"{filtered_df['product_code'].nunique():,}"
                ]
            })
            st.dataframe(order_stats_df, hide_index=True, use_container_width=True)
    
    # Footer
    st.markdown("---")
    
    # Automated Business Insights
    st.header("Automated Business Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Best Performers")
        top_product_line = filtered_df.groupby('product_line')['sales'].sum().idxmax()
        top_product_revenue = filtered_df.groupby('product_line')['sales'].sum().max()
        st.success(f"**Top Product Line:** {top_product_line}")
        st.info(f"Revenue: ${top_product_revenue:,.2f}")
        
        top_country = filtered_df.groupby('country')['sales'].sum().idxmax()
        top_country_revenue = filtered_df.groupby('country')['sales'].sum().max()
        st.success(f"**Top Country:** {top_country}")
        st.info(f"Revenue: ${top_country_revenue:,.2f}")
    
    with col2:
        st.subheader("Sales Patterns")
        best_quarter = filtered_df.groupby('qtr_id')['sales'].sum().idxmax()
        st.success(f"**Strongest Quarter:** Q{best_quarter}")
        
        best_month = filtered_df.groupby('month_name')['sales'].sum().idxmax()
        st.success(f"**Best Month:** {best_month}")
        
        most_common_deal = filtered_df['deal_size'].mode()[0]
        st.info(f"**Most Common Deal Size:** {most_common_deal}")
    
    with col3:
        st.subheader("Revenue Insights")
        total_revenue = filtered_df['sales'].sum()
        total_quantity = filtered_df['quantity_ordered'].sum()
        avg_revenue_per_item = total_revenue / total_quantity if total_quantity > 0 else 0
        
        st.metric("Avg Revenue/Item", f"${avg_revenue_per_item:.2f}")
        
        # Calculate revenue concentration (top 20% customers)
        customer_revenue = filtered_df.groupby('customer_name')['sales'].sum().sort_values(ascending=False)
        top_20_pct_customers = int(len(customer_revenue) * 0.2)
        top_20_pct_revenue = customer_revenue.head(top_20_pct_customers).sum()
        concentration = (top_20_pct_revenue / total_revenue) * 100
        
        st.info(f"**Top 20% customers generate:**")
        st.success(f"{concentration:.1f}% of revenue")

if __name__ == "__main__":
    main()