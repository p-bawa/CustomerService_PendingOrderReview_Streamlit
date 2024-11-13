##########
# Imports
##########
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import when_matched
import time
import pandas as pd
from datetime import datetime, timedelta
# from streamlit_option_menu import option_menu


session = get_active_session()
st.set_page_config(page_title="Data Editor", layout="wide")


##### SQL
TABLE_NAME = "DEVELOPMENT.CUSTOMER_SERVICE.STREAMLIT_CS"
COL_PK = "ID"

COL_NEW_SUBMITTER_EMAIL_ID = "NEW_SUBMITTER_EMAIL_ID"
# COL_NEW_REVIEWER_EMAIL_ID = "NEW_REVIEWER_EMAIL_ID"
# COL_ORDERED_DATE = 'ORDERED_DATE'
COL_REMOVE = "REMOVE"
COL_NOTES = "NOTES"

##### Messages
MSG_UPDATE_SUCCESS = "Records updated"
MSG_UPDATE_ERROR = "Failure Updating Records. Contact BI"

TITLE = ":chart_with_upwards_trend: Sales Order Review "
MSG_INSTRUCTIONS = f"""
This app let's you edit submitter and reviewer email id for line order for accurate reporting

## :white_check_mark: Editable Columns
1. **NEW_SUBMITTER_EMAIL_ID**: Enter the corrected email id for submitter by selecting a value from the user dropdown.
1. **NEW_REVIEWER_EMAIL_ID**: Enter the corrected email id for the reviewer by selecting a value from the user dropdown.


---
Expand the table as needed. When done, hit `Submit` and confirm you receive the '{MSG_UPDATE_SUCCESS}' message
"""
MSG_CLOSE = "Feel free to contact us at pbawa@shockwavemedical.com or slala@shockwavemedical.com! :smile:"


SUBTITLE_FILTERS = "## Order Date"

SUBTITLE_TABLE = "## Records"

session = get_active_session()


# Load distinct emails from users table
@st.cache_data
def load_emails():
    query = "SELECT DISTINCT email FROM DEVELOPMENT.SALESFORCE.SALESFORCE_USER"
    test = session.sql(query).collect()
    # email_list = [].split(',')
    email_list = []
    for email in test:
        email_list.append(email)
    return email_list


##########
# StreamLit Configs
##########
COL_CONFIG = {
     'ID' : st.column_config.NumberColumn(
        help = "ROW_ID",
        disabled = True,
        format="%f"
    ),
     'SALES_ORDER_NUMBER' : st.column_config.NumberColumn(
        help = "SALES_ORDER_NUMBER",
        disabled = True,
        format="%f"
    ),

    'SUBMITTED_DATE': st.column_config.DateColumn(
        help = "Submitted Date",
        disabled = True,
    ), 
    'SUBMITTER_EMAIL_ID': st.column_config.TextColumn(
        help = "Submitter Email ID",
        disabled = True
        
    ),
    COL_NEW_SUBMITTER_EMAIL_ID: st.column_config.SelectboxColumn(
        options = load_emails()
    ), 
    'REVIEWER_EMAIL_ID': st.column_config.TextColumn(
        help = ""
    ), 
   
     'HOLD_APPLIED_DATE': st.column_config.DateColumn(
        help = "Hold Applied Date",
        disabled = True,
    ),
     'REMOVE': st.column_config.CheckboxColumn(
        help = "Whether to exclude sales order from reporting"
    )
    
}

st.title("📦Pending Order Review")
st.subheader("✨ Overview")
st.write("""
Easily manage Submitter Email IDs for pending orders to ensure precise tracking. 
""")
dataset = session.table(TABLE_NAME)
df = pd.DataFrame(dataset.collect())
df['SUBMITTED_DATE'] = pd.to_datetime(df['SUBMITTED_DATE']) 
df['HOLD_APPLIED_DATE'] = pd.to_datetime(df['HOLD_APPLIED_DATE'])
# df = df[['ID', 'ORDERED_DATE','SALES_ORDER_NUMBER', 'SUBMITTER_EMAIL_ID', 'NEW_SUBMITTER_EMAIL_ID', 'REVIEWER_EMAIL_ID', 'NEW_REVIEWER_EMAIL_ID', 'SOURCE_ORDER_NUMBER', 'SALES_ORDER_TYPE', 'HOLD_APPLIED_BY', 'HOLD_APPLIED_DATE',
#     'HOLD_RELEASED_BY', 'RELEASE_REASON', 'REMOVE', 'NOTES']]
##########
# Filters
##########


# UI filters for the email and line ID
st.caption("Filter orders based on the following:")

# Get today's date and the date 10 days ago
hold_applied_min = min(df['HOLD_APPLIED_DATE'])
hold_applied_max = max(df['HOLD_APPLIED_DATE'])

# Get unique values from the dataframe columns for dynamic filtering
unique_submitter_emails = df['SUBMITTER_EMAIL_ID'].unique()
unique_reviewer_emails = df['REVIEWER_EMAIL_ID'].unique()
unique_sales_order_num = df['SALES_ORDER_NUMBER'].unique()
unique_source_order_num = df['SOURCE_ORDER_NUMBER'].unique()
unique_sales_order_type = df['SALES_ORDER_TYPE'].unique()
unique_hold_applied_by = df['HOLD_APPLIED_BY'].unique()
unique_hold_released_by = df['HOLD_RELEASED_BY'].unique()
unique_hold_applied_date = df['HOLD_APPLIED_DATE'].unique()
unique_release_reason = df['RELEASE_REASON'].unique()


# Create columns to place filters in one row
col1, col2, col3, col4, col5, col6, col7= st.columns(7)
col8, col9, col10, col11, col12 = st.columns(5)
# Filter by submitter email (with a selectbox that lists all unique submitter emails)
with col1:
    submitter_email = st.selectbox(
    'Submitter Email ID', 
    options=['All'] + list(unique_submitter_emails),  # 'All' allows for no filter
    index=0  # Default to 'All'
)

# Filter by reviewer email (with a selectbox that lists all unique reviewer emails)
with col2:
    reviewer_email = st.selectbox(
    'Reviewer Email ID', 
    options=['All'] + list(unique_reviewer_emails),  # 'All' allows for no filter
    index=0  # Default to 'All'
)

# Filter by line ID (with a multiselect that lists all unique line IDs)
with col3:
    sales_order_num = st.selectbox(
    'Sales Order Number',
      options=['All'] + [str(order_num) for order_num in unique_sales_order_num],  # Convert to string for selectbox
    index=0  
)
with col4:
    source_order_num = st.selectbox(
    'Source Order Number',
      options=['All'] + list(unique_source_order_num),  # Convert to string for selectbox
    index=0  
)

with col5:
    sales_order_type = st.selectbox(
    'Sales Order Type',
      options=['All'] + list(unique_sales_order_type),  # Convert to string for selectbox
    index=0  
)

# Date filter
with col6:
# Get today's date and the date 10 days ago
    min_date = min(df['SUBMITTED_DATE'])
    max_date = max(df['SUBMITTED_DATE'])

#Set the default start and end dates
    start_date = st.date_input(
    label='Submitted Date (FROM)', 
    value=min_date,  # Default to 10 days ago
    format='MM/DD/YYYY'
)

with col7:
    end_date = st.date_input(
    label='Submitted End Date (TO)', 
    value=max_date,  # Default to today
    format='MM/DD/YYYY'
)
with col8:
    hold_applied_by = st.selectbox(
    'Hold Applied By',
     options=['All'] + list(unique_hold_applied_by),
    index=0  
)
with col9:
    hold_applied_start_date = st.date_input(
    label='Hold Applied Date (FROM)', 
    value=hold_applied_min,  # Default to today
    format='MM/DD/YYYY'
)    
with col10:
    hold_applied_end_date = st.date_input(
    label='Hold Applied Date (TO)', 
    value=hold_applied_max,  # Default to today
    format='MM/DD/YYYY'
)
with col11:
    hold_released_by = st.selectbox(
'Hold Released By',
    options=['All'] + list(unique_hold_released_by),
    index=0 
)
with col12:
    release_reason = st.selectbox(
'Release Reason',
    options=['All'] + list(unique_release_reason),
    index=0 
)
# Filter based on submitter email
if submitter_email != 'All':
    df = df[df['SUBMITTER_EMAIL_ID'] == submitter_email]

# Filter based on reviewer email
if reviewer_email != 'All':
    df = df[df['REVIEWER_EMAIL_ID'] == reviewer_email]

# Filter based on reviewer email
if source_order_num != 'All':
    df = df[df['SOURCE_ORDER_NUMBER'] == source_order_num]
# Filter based on reviewer email
if sales_order_type != 'All':
    df = df[df['SALES_ORDER_TYPE'] == sales_order_type]

# Apply the filter based on selected dates
df = df[
(df['SUBMITTED_DATE'] >= pd.to_datetime(start_date)) & 
(df['SUBMITTED_DATE'] <= pd.to_datetime(end_date))
]

# Filter based on multiple selected line IDs
if sales_order_num != 'All':
    sales_order_num = int(sales_order_num)
    df = df[df['SALES_ORDER_NUMBER'] == sales_order_num]

if hold_applied_by != 'All':
    df = df[df['HOLD_APPLIED_BY'] == hold_applied_by]

# Apply the filter based on selected dates
df = df[
(df['HOLD_APPLIED_DATE'] >= pd.to_datetime(hold_applied_start_date)) & 
(df['HOLD_APPLIED_DATE'] <= pd.to_datetime(hold_applied_end_date))
]

if hold_released_by != 'All':
    df = df[df['HOLD_RELEASED_BY'] == hold_released_by]

if release_reason != 'All':
    df = df[df['RELEASE_REASON'] == release_reason]




##########
# Edit Data Form
##########
st.caption(SUBTITLE_TABLE)

with st.form("Edit Submitter Email IDs, Remove Flag and add notes if needed."):
    st.caption("Edit Submitter Email IDs, Remove Flag and add notes if needed.")
    edited = st.data_editor(
    data= df,
    use_container_width=True, 
    hide_index=True, 
    column_config=COL_CONFIG
)
    submit_button = st.form_submit_button()

st.caption(MSG_CLOSE)

if submit_button:
    try:
        st.warning("Attempting to update dataset")
        updated_dataset = session.create_dataframe(edited)
    # Perform the merge operation
        dataset.merge( 
            source=updated_dataset, 
            join_expr=(dataset[COL_PK] == updated_dataset[COL_PK]),
            clauses=[
            when_matched().update({
                COL_NEW_SUBMITTER_EMAIL_ID: updated_dataset[COL_NEW_SUBMITTER_EMAIL_ID], 
                # COL_NEW_REVIEWER_EMAIL_ID: updated_dataset[COL_NEW_REVIEWER_EMAIL_ID],
                COL_REMOVE: updated_dataset[COL_REMOVE],
                COL_NOTES: updated_dataset[COL_NOTES]
            })
        ]
    )
    # Provide user feedback after merge success
        st.success(MSG_UPDATE_SUCCESS)
        time.sleep(.5)
        st.experimental_rerun()

    except Exception as e:
    # Output error details for debugging
        st.error(f"{MSG_UPDATE_ERROR}: {e}")

        st.experimental_rerun()
