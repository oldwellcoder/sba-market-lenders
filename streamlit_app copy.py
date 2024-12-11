import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
from io import StringIO

load_dotenv()

st.set_page_config(page_title="SBALend.ai", page_icon="📊", layout="wide")

# Additional CSS
st.markdown("""
<style>
            
header {visibility: hidden;}
.streamlit-footer {display: none;}
@import url('https://api.fontshare.com/v2/css?f[]=satoshi@700,500,400&f[]=zodiak@700,400&display=swap');

[data-testid="stVerticalBlock"] {
   padding: 0;
}

.stApp {
   max-width: 1400px;
   margin: 0 auto;
   padding: 0 120px;
}

h1 { 
   font-family: 'Zodiak', serif !important;
   font-size: 80px !important;
   line-height: 120% !important;
   color: #25221c !important;
}
h2 { 
   font-family: 'Zodiak', serif !important;
   font-size: 40px !important;
   line-height: 54px !important;
   color: #25221c !important;
}
h3 { 
   font-family: 'Zodiak', serif !important;
   font-size: 24px !important;
   line-height: 32px !important;
   color: #25221c !important;
}
h4 { 
   font-family: 'Satoshi', sans-serif !important;
   font-size: 18px !important;
   line-height: 24px !important;
   color: #25221c !important;
}

.stSelectbox div [data-baseweb="select"] {
   font-family: 'Satoshi', sans-serif !important;
   font-size: 24px !important;
}

/* Remove red outline on focus */
:focus, :focus-visible {
   outline: none !important;
   box-shadow: none !important;
}

.metric-container {
   background: #f8f1e3;
   padding: 24px;
   border-radius: 8px;
   margin: 0 12px 24px 12px;
   height: 160px;
   display: flex;
   flex-direction: column;
}

.metric-label {
   font-family: 'Satoshi', sans-serif !important;
   font-size: 14px !important;
   line-height: 20px !important;
   color: #302d27 !important;
}

.metric-value {
   font-family: 'Zodiak', serif !important;
   font-size: 40px !important;
   line-height: 54px !important;
   color: #25221c !important;
   flex-grow: 1;
   display: flex;
   align-items: center;
}

/* Table styling */
.dataframe {
   font-family: 'Satoshi', sans-serif;
   font-size: 16px;
   line-height: 135%;
   width: 100%;   /* Make the table take full width inside stApp */
   margin: 0 auto;
   background: white;
   border-radius: 8px;
   border-collapse: separate;
   border-spacing: 0;
}

.dataframe th {
   padding: 24px;
   text-align: left;
   background: #f8f1e3;
   color: #25221c;
}

.dataframe td {
   padding: 24px;
   border-top: 1px solid #f8f1e3;
   white-space: normal;
   word-wrap: break-word;
}

.pill {
   display: inline-block;
   padding: 4px 12px;
   border-radius: 16px;
   font-family: 'Satoshi', sans-serif;
   font-size: 14px;
   line-height: 20px;
   font-weight: 500;
   text-align: center;
   min-width: 80px;
}
</style>
""", unsafe_allow_html=True)


def get_percentile_rating(val):
   if pd.isna(val):
       return "N/A", "#f8f1e3"
   val = float(val)
   if val >= 90:
       return "Very Low", "#f6b762"
   elif val >= 75:
       return "Low", "#f7c27f"
   elif val >= 60:
       return "Below Average", "#f8cd9c"
   elif val >= 40:
       return "Average", "#f9d8b9"
   elif val >= 25:
       return "Above Average", "#fae3d6"
   elif val >= 10:
       return "High", "#fbeed3"
   else:
       return "Very High", "#fcf7ed"

@st.cache_resource(show_spinner=False)
def get_snowflake_connection():
   return snowflake.connector.connect(
       user=os.getenv('SNOWFLAKE_USER'),
       password=os.getenv('SNOWFLAKE_PASSWORD'),
       account=os.getenv('SNOWFLAKE_ACCOUNT'),
       warehouse=f"SBA_WH_{os.getenv('ENVIRONMENT', 'dev').upper()}",
       database=f"SBA_DB_{os.getenv('ENVIRONMENT', 'dev').upper()}",
       schema='REFINED_SBA7A_PUBLIC',
       role=f"SBA_ROLE_{os.getenv('ENVIRONMENT', 'dev').upper()}"
   )

@st.cache_data(show_spinner=False)
def fetch_data(_conn, query):
   return pd.read_sql(query, _conn)

conn = get_snowflake_connection()

st.header("SBA 7(a) Lender Analysis")

size_options = {
   'Total': 'All Loan Sizes',
   '<=50k': '$50 Thousand or Less',
   '50k-500k': '$50 to $500 Thousand',
   '>500k': 'Greater than $500 Thousand'
}

col1, col2 = st.columns(2)
with col1:
   size_bucket = st.selectbox(
       "Loan Size",
       options=list(size_options.keys()),
       format_func=lambda x: size_options[x],
       index=0
   )

with col2:
   with st.spinner('Loading industries...'):
       industry_query = """
       SELECT DISTINCT INDUSTRY 
       FROM SBA7A_PRICING_INDUSTRY_TABLE 
       WHERE INDUSTRY != 'All Industries'
       AND LOANS_APPROVED > 0 
       ORDER BY INDUSTRY
       """
       industries = ['All Industries'] + fetch_data(conn, industry_query)['INDUSTRY'].tolist()
   industry = st.selectbox("Industry", options=industries, index=0)

with st.spinner('Loading metrics...'):
   bans_query = f"""
   SELECT LOANS_APPROVED, LOANS_APPROVED_AMT, PCT_VARIABLE, PRICING_PERCENTILE, TYPICAL_PRICING
   FROM SBA7A_PRICING_INDUSTRY_TABLE
   WHERE SIZE_BUCKET = '{size_bucket}'
   AND INDUSTRY = '{industry}'
   """
   bans_data = fetch_data(conn, bans_query)

col1, col2, col3, col4 = st.columns(4)
with col1:
   st.markdown(f"""
   <div class="metric-container">
       <div class="metric-label">Loans Approved (3 Year)</div>
       <div class="metric-value">{bans_data['LOANS_APPROVED'].iloc[0]:,}</div>
   </div>
   """, unsafe_allow_html=True)
   
with col2:
   st.markdown(f"""
   <div class="metric-container">
       <div class="metric-label">Approved Amount (3 Year)</div>
       <div class="metric-value">${bans_data['LOANS_APPROVED_AMT'].iloc[0]/1_000_000:,.0f}M</div>
   </div>
   """, unsafe_allow_html=True)

with col3:
   pct_fixed = "N/A" if pd.isna(bans_data['PCT_VARIABLE'].iloc[0]) else f"{1 - bans_data['PCT_VARIABLE'].iloc[0]/100:.0%}"
   st.markdown(f"""
   <div class="metric-container">
       <div class="metric-label">Percent Fixed Rate</div>
       <div class="metric-value">{pct_fixed}</div>
   </div>
   """, unsafe_allow_html=True)
   
with col4:
   percentile = bans_data['PRICING_PERCENTILE'].iloc[0]
   if industry == "All Industries":
       display_percentile = "N/A"
   else:
       rating, color = get_percentile_rating(percentile)
       display_percentile = f'<div style="font-family:Zodiak,serif;font-size:40px;line-height:54px;color:#25221c;">{rating}</div>'
   st.markdown(f"""
   <div class="metric-container">
       <div class="metric-label">Average Pricing</div>
       <div class="metric-value">{display_percentile}</div>
   </div>
   """, unsafe_allow_html=True)

with st.spinner('Loading lenders...'):
   lenders_query = f"""
   SELECT 
       LENDER,
       LOANS_APPROVED,
       LOANS_APPROVED_AMT,
       PCT_VARIABLE,
       PRICING_PERCENTILE,
       TYPICAL_PRICING
   FROM SBA7A_PRICING_LENDER_TABLE
   WHERE SIZE_BUCKET = '{size_bucket}'
   AND INDUSTRY = '{industry}'
   ORDER BY LOANS_APPROVED DESC
   LIMIT 50
   """
   lenders_data = fetch_data(conn, lenders_query)

column_headers = {
   'LENDER': 'Lender',
   'LOANS_APPROVED': 'Loans Approved (3 Year)',
   'LOANS_APPROVED_AMT': 'Approved Amount (3 Year)',
   'PCT_VARIABLE': 'Percent Fixed Rate',
   'PRICING_PERCENTILE': 'Average Pricing',
   'TYPICAL_PRICING': 'Typical Pricing'
}

lenders_data_formatted = lenders_data.copy()
lenders_data_formatted['LOANS_APPROVED'] = lenders_data_formatted['LOANS_APPROVED'].apply(lambda x: f"{x:,}")
lenders_data_formatted['LOANS_APPROVED_AMT'] = lenders_data_formatted['LOANS_APPROVED_AMT'].apply(lambda x: f"${x/1_000_000:,.1f}M")
lenders_data_formatted['PCT_VARIABLE'] = lenders_data_formatted['PCT_VARIABLE'].apply(lambda x: f"{(1-x/100):.0%}" if pd.notnull(x) else "N/A")

# Restore pills for Average Pricing
lenders_data_formatted['PRICING_PERCENTILE'] = lenders_data_formatted['PRICING_PERCENTILE'].apply(
    lambda x: f'<div class="pill" style="background-color: {get_percentile_rating(x)[1]}; color: #25221c">{get_percentile_rating(x)[0]}</div>' if pd.notnull(x) else "N/A"
)

lenders_data_formatted['TYPICAL_PRICING'] = lenders_data_formatted['TYPICAL_PRICING'].fillna('N/A')

df_renamed = lenders_data_formatted.rename(columns=column_headers)

st.markdown("### Top Lenders")

# Export to CSV
csv_buffer = StringIO()
df_renamed.to_csv(csv_buffer, index=False)
st.download_button(
    label="Export to CSV",
    data=csv_buffer.getvalue(),
    file_name='top_lenders.csv',
    mime='text/csv'
)

# Show only top 20 rows as an HTML table
df_20 = df_renamed.head(20)

st.write(df_20.to_html(
   escape=False,
   index=False,
   classes=['dataframe'],
   justify='left'
), unsafe_allow_html=True)
