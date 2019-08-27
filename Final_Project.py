import pandas as pd
import numpy as np
import os

prod_master = pd.read_csv("PROD MASTER.csv", usecols=['PROD_NBR', 'PROD_DESC', 'MAJOR_CAT_CD', 'CAT_CD'],
                          dtype={'PROD_NBR': str, 'CAT_CD': str})

major_prod_cat = pd.read_csv("MAJOR PROD CAT.csv", usecols=['MAJOR_CAT_CD', 'MAJOR_CAT_DESC'],
                             dtype={'MAJOR_CAT_CD': str})
# major_prod_cat.columns = ['MAJOR_CAT_CD', 'MAJOR_CAT_DESC']

prod_cat = pd.read_csv("PROD CAT.csv", usecols=['CAT_CD', 'CAT_DESC', 'MAJOR_CAT_CD'], dtype={'CAT_CD': str})
# prod_cat.columns = ['CAT_CD', 'CAT_DESC', 'MAJOR_CAT_CD']

pos_trans = pd.read_csv("POS_TRANS.csv", usecols=['BSKT_ID', 'PHRMCY_NBR', 'PROD_NBR', 'SLS_DTE_NBR', 'EXT_SLS_AMT', 'SLS_QTY'],
                        dtype={'PROD_NBR': str})
# pos_trans.columns = ['BSKT_ID', 'PHRMCY_NBR', 'PROD_NBR', 'SLS_DTE_NBR', 'EXT_SLS_AMT', 'SLS_QTY']

phrmcy_master = pd.read_csv("PHRMCY_MASTER.csv", usecols=['PHRMCY_NBR', 'ST_CD', 'ZIP_3_CD'])

# Merge Transactions and Products
merged_df = pd.merge(pos_trans, prod_master, on='PROD_NBR')

# Merge Transactions and Pharmacy dataframes
merged_df = pd.merge(merged_df, phrmcy_master, on='PHRMCY_NBR')

# Correct date to pandas datetime
merged_df['SLS_DTE_NBR'] = pd.to_datetime(merged_df['SLS_DTE_NBR'], format='%Y%m%d')

# Add column for Month
merged_df['month'] = merged_df['SLS_DTE_NBR'].dt.month

# Add column for line item sale amount = sls_amt + sls_qty
merged_df['line_item_sale'] = merged_df.EXT_SLS_AMT * merged_df.SLS_QTY

# Get State Names
states = merged_df['ST_CD'].drop_duplicates()

# Get months
months = merged_df['month'].drop_duplicates()

# Initialize DataFrames for Output information
category_df = pd.DataFrame([])
major_category_df = pd.DataFrame([])
state_average_df = pd.DataFrame([])
zip_sales_df = pd.DataFrame([])
zip_sales_max_df = pd.DataFrame([])
zip_sales_min_df = pd.DataFrame([])
major_category_leading = pd.DataFrame([])
category_leading = pd.DataFrame([])
prod_leading = pd.DataFrame([])
major_category_month_leading = pd.DataFrame([])
category_month_leading = pd.DataFrame([])
prod_month_leading = pd.DataFrame([])
month_max_min_df = pd.DataFrame([])

prod_leading_rev = pd.DataFrame([])
prod_month_leading_rev = pd.DataFrame([])

month_state_combined_df = pd.DataFrame([])

# Gather state by state information
for state in states:
    # Data frame of all sales in state
    state_df = merged_df[merged_df.ST_CD == state]

    # Data Frame of all stores within state
    stores_df = pd.DataFrame(state_df['PHRMCY_NBR'].drop_duplicates())
    total_sales = sum(state_df['line_item_sale'])
    average_sales = total_sales / len(stores_df)

    # Calculate average sales per store for state
    state_avg = pd.DataFrame(state_df['ST_CD'].drop_duplicates())
    state_avg['total_sales'] = total_sales
    state_avg['number_stores'] = len(stores_df)
    state_avg['average_sales'] = average_sales
    state_average_df = pd.concat([state_average_df, state_avg])

    # Sum of all sales in each category (for the state)
    state_categories = pd.DataFrame(state_df['CAT_CD'].drop_duplicates())
    state_categories['SLS_QTY'] = [sum(state_df[state_df['CAT_CD'] == category]['SLS_QTY']) for category in
                                   state_categories['CAT_CD']]
    state_categories['SLS_AMT'] = [sum(state_df[state_df['CAT_CD'] == category]['line_item_sale']) for category in
                                   state_categories['CAT_CD']]
    state_categories['pct_state'] = state_categories['SLS_AMT'] / total_sales
    state_categories['ST_CD'] = [state] * len(state_categories['CAT_CD'])
    category_df = pd.concat([category_df, state_categories])

    # Sum of all sales in each major category (for the state)
    state_major_categories = pd.DataFrame(state_df['MAJOR_CAT_CD'].drop_duplicates())
    state_major_categories['SLS_QTY'] = [sum(state_df[state_df['MAJOR_CAT_CD'] == major_category]['SLS_QTY']) for
                                         major_category in state_major_categories['MAJOR_CAT_CD']]
    state_major_categories['SLS_AMT'] = [sum(state_df[state_df['MAJOR_CAT_CD'] == major_category]['line_item_sale']) for
                                         major_category in state_major_categories['MAJOR_CAT_CD']]
    state_major_categories['pct_state'] = state_major_categories['SLS_AMT'] / total_sales
    state_major_categories['ST_CD'] = [state] * len(state_major_categories['MAJOR_CAT_CD'])
    major_category_df = pd.concat([major_category_df, state_major_categories])

    # Calculate total sales for each zip code
    state_zips = pd.DataFrame(state_df['ZIP_3_CD'].drop_duplicates())
    state_zips['SLS_AMT'] = [sum(state_df[state_df['ZIP_3_CD'] == zip_cd]['line_item_sale']) for
                             zip_cd in state_zips['ZIP_3_CD']]
    state_zips['pct_state'] = state_zips['SLS_AMT'] / total_sales
    state_zips['ST_CD'] = [state] * len(state_zips['ZIP_3_CD'])
    state_largest_zip = state_zips[state_zips['SLS_AMT'] == state_zips['SLS_AMT'].max()]
    state_smallest_zip = state_zips[state_zips['SLS_AMT'] == state_zips['SLS_AMT'].min()]
    zip_sales_max_df = pd.concat([zip_sales_max_df, state_largest_zip])
    zip_sales_min_df = pd.concat([zip_sales_min_df, state_smallest_zip])
    zip_sales_df = pd.concat([zip_sales_df, state_zips])

    # 1) Leading Major categories in each state:
    state_major_categories = state_major_categories.astype({'MAJOR_CAT_CD': str})
    state_major_categories = pd.merge(state_major_categories, major_prod_cat, on='MAJOR_CAT_CD')
    state_largest_maj_cat = state_major_categories[
        state_major_categories['SLS_AMT'] == state_major_categories['SLS_AMT'].max()]
    major_category_leading = pd.concat([major_category_leading, state_largest_maj_cat])

    # Leading Category in each state
    state_categories = state_categories.astype({'CAT_CD': str})
    state_categories = pd.merge(state_categories, prod_cat, on='CAT_CD')
    state_largest_cat = state_categories[state_categories['SLS_AMT'] == state_categories['SLS_AMT'].max()]
    category_leading = pd.concat([category_leading, state_largest_cat])

    # Leading Product in leading category in each state
    product_leading_cat_df = state_df[state_df['CAT_CD'] == state_largest_cat['CAT_CD'].values[0]]
    state_products = pd.DataFrame(product_leading_cat_df['PROD_NBR'].drop_duplicates())
    state_products = state_products.astype({'PROD_NBR': str})
    state_products = pd.merge(state_products, prod_master, on='PROD_NBR')
    state_products['SLS_AMT'] = [
        sum(product_leading_cat_df[product_leading_cat_df['PROD_NBR'] == product]['line_item_sale']) for product in
        state_products['PROD_NBR']]
    state_products['pct_state'] = state_products['SLS_AMT'] / total_sales
    state_products['ST_CD'] = [state] * len(state_products['PROD_NBR'])
    state_largest_cat_largest_prod = state_products[state_products['SLS_AMT'] == state_products['SLS_AMT'].max()]
    prod_leading = pd.concat([prod_leading, state_largest_cat_largest_prod])

    # Leading Products in each state (NOTE: HIGHLY MEMORY INTENSIVE)
    # state_products_rev = pd.DataFrame(state_df['PROD_NBR'].drop_duplicates())
    # state_products_rev = state_products_rev.astype({'PROD_NBR': str})
    # state_products_rev = pd.merge(state_products_rev, prod_master, on='PROD_NBR')
    # state_products_rev['SLS_AMT'] = [
    #     sum(state_df[state_df['PROD_NBR'] == product]['line_item_sale']) for product in
    #     state_products_rev['PROD_NBR']]
    # state_products_rev['pct_state'] = state_products_rev['SLS_AMT'] / total_sales
    # state_products_rev['ST_CD'] = [state] * len(state_products_rev['PROD_NBR'])
    # state_largest_cat_largest_prod_rev = state_products_rev[state_products_rev['SLS_AMT'] == state_products_rev['SLS_AMT'].max()]
    # prod_leading_rev = pd.concat([prod_leading_rev, state_largest_cat_largest_prod_rev])

    month_state_total_df = pd.DataFrame([])
    # Gather highest major categories for each month/state
    for month in months:
        state_month_df = state_df[state_df['month'] == month]

        # Month total sales
        month_total_sales = pd.DataFrame({'ST_CD': [state],
                                          'month': [month],
                                          'SLS_AMT': [sum(state_month_df['line_item_sale'])]
                                          })

        month_state_total_df = pd.concat([month_state_total_df, month_total_sales])

        # Major Category in each state in each month
        state_month_maj_cat = pd.DataFrame(state_month_df['MAJOR_CAT_CD'].drop_duplicates())
        state_month_maj_cat['SLS_AMT'] = [
            sum(state_month_df[state_month_df['MAJOR_CAT_CD'] == major_category]['line_item_sale']) for major_category
            in
            state_month_maj_cat['MAJOR_CAT_CD']]
        state_month_maj_cat['ST_CD'] = [state] * len(state_month_maj_cat['MAJOR_CAT_CD'])
        state_month_maj_cat['month'] = [month] * len(state_month_maj_cat['MAJOR_CAT_CD'])
        state_month_largest_maj_cat = state_month_maj_cat[
            state_month_maj_cat['SLS_AMT'] == state_month_maj_cat['SLS_AMT'].max()]
        state_month_largest_maj_cat = state_month_largest_maj_cat.astype({'MAJOR_CAT_CD': str})
        state_month_largest_maj_cat = pd.merge(state_month_largest_maj_cat, major_prod_cat, on='MAJOR_CAT_CD')
        major_category_month_leading = pd.concat([major_category_month_leading, state_month_largest_maj_cat])

        # Leading Category in each state in each month
        state_month_cat = pd.DataFrame(state_month_df['CAT_CD'].drop_duplicates())
        state_month_cat['SLS_AMT'] = [sum(state_month_df[state_month_df['CAT_CD'] == category]['line_item_sale']) for
                                      category in
                                      state_month_cat['CAT_CD']]
        state_month_cat['ST_CD'] = [state] * len(state_month_cat['CAT_CD'])
        state_month_cat['month'] = [month] * len(state_month_cat['CAT_CD'])
        state_month_largest_cat = state_month_cat[state_month_cat['SLS_AMT'] == state_month_cat['SLS_AMT'].max()]
        state_month_largest_cat = state_month_largest_cat.astype({'CAT_CD': str})
        state_month_largest_cat = pd.merge(state_month_largest_cat, prod_cat, on='CAT_CD')
        category_month_leading = pd.concat([category_month_leading, state_month_largest_cat])

        # Leading Product in each state in each month (NOTE: HIGHLY MEMORY INTENSIVE)
        # state_month_products = pd.DataFrame(state_month_df['PROD_NBR'].drop_duplicates())
        # state_month_products = state_month_products.astype({'PROD_NBR': str})
        # state_month_products = pd.merge(state_month_products, prod_master, on='PROD_NBR')
        # state_month_products['SLS_AMT'] = [
        #     sum(state_month_df[state_month_df['PROD_NBR'] == product]['line_item_sale']) for
        #     product in
        #     state_month_products['PROD_NBR']]
        # state_month_products['ST_CD'] = [state] * len(state_month_products['PROD_NBR'])
        # state_month_products['month'] = [month] * len(state_month_products['PROD_NBR'])
        # state_month_products_largest = state_month_products[
        #     state_month_products['SLS_AMT'] == state_month_products['SLS_AMT'].max()]
        # state_month_products_largest = state_month_products_largest.astype({'PROD_NBR': str})
        # prod_month_leading_rev = pd.concat([prod_month_leading_rev, state_month_products_largest])

        # Leading Product in leading category in each state
        month_product_leading_cat_df = state_month_df[
            state_month_df['CAT_CD'] == state_month_largest_cat['CAT_CD'].values[0]]
        state_month_products = pd.DataFrame(month_product_leading_cat_df['PROD_NBR'].drop_duplicates())
        state_month_products = state_month_products.astype({'PROD_NBR': str})
        state_month_products = pd.merge(state_month_products, prod_master, on='PROD_NBR')
        state_month_products['SLS_AMT'] = [
            sum(month_product_leading_cat_df[month_product_leading_cat_df['PROD_NBR'] == product]['line_item_sale']) for
            product in
            state_month_products['PROD_NBR']]
        state_month_products['ST_CD'] = [state] * len(state_month_products['PROD_NBR'])
        state_month_products['month'] = [month] * len(state_month_products['PROD_NBR'])
        state_month_products_cat = state_month_products[
            state_month_products['SLS_AMT'] == state_month_products['SLS_AMT'].max()]
        state_month_products_cat = state_month_products_cat.astype({'PROD_NBR': str})
        prod_month_leading = pd.concat([prod_month_leading, state_month_products_cat])

    # Gather month sales data for state
    month_state_combined_df = pd.concat([month_state_combined_df, month_state_total_df])
    month_state_max = month_state_total_df[month_state_total_df['SLS_AMT'] == month_state_total_df['SLS_AMT'].max()]
    month_state_min = month_state_total_df[month_state_total_df['SLS_AMT'] == month_state_total_df['SLS_AMT'].min()]
    month_max_min_df = pd.concat([month_max_min_df, month_state_max, month_state_min])


# How do Months vary from state to state
month_state_variance_list = []
# Get State Month statistics
for state in states:
    month_state_df = month_state_combined_df[month_state_combined_df['ST_CD'] == state]
    state_variance = month_state_df['SLS_AMT'].describe().tolist()
    month_state_max = str(month_state_df[month_state_df['SLS_AMT'] == month_state_df['SLS_AMT'].max()]['month'].values[0])
    month_state_min = str(month_state_df[month_state_df['SLS_AMT'] == month_state_df['SLS_AMT'].min()]['month'].values[0])
    state_variance.insert(0,state)
    state_variance.insert(5,month_state_min)
    state_variance.insert(10,month_state_max)
    month_state_variance_list.append(state_variance)
state_month_variance = pd.DataFrame(month_state_variance_list, columns=['ST_CD','count','mean','std','min','min_month','25','50','75','max','max_month'])
state_month_variance = state_month_variance.sort_values(by=['std'],ascending=False)

# Sort leading state/month data by states and month
prod_month_leading = prod_month_leading.sort_values(by=['ST_CD','month'],ascending=True)
# prod_month_leading_rev = prod_month_leading_rev.sort_values(by=['ST_CD','month'],ascending=True)
category_month_leading = category_month_leading.sort_values(by=['ST_CD','month'],ascending=True)
major_category_month_leading = major_category_month_leading.sort_values(by=['ST_CD','month'],ascending=True)


# How do Major Product Categories vary from state to state
major_category_df = major_category_df.astype({'MAJOR_CAT_CD': str})
major_category_variance_list = []
# Note: Taking out Deleware from statistics because of low amount of sales
major_category_no_de_df = major_category_df[major_category_df['ST_CD'] != 'DE']
major_categories = pd.DataFrame(major_category_no_de_df['MAJOR_CAT_CD'].drop_duplicates())
# Get Major Product Category statistics
for major_category in major_categories['MAJOR_CAT_CD']:
    major_cat_df = major_category_no_de_df[major_category_no_de_df['MAJOR_CAT_CD'] == major_category]
    major_cat_variance = major_cat_df['SLS_AMT'].describe().tolist()
    major_cat_variance.insert(0,major_category)
    major_category_variance_list.append(major_cat_variance)
major_category_variance = pd.DataFrame(major_category_variance_list, columns=['MAJOR_CAT_CD','count','mean','std','min','25','50','75','max'])
major_category_variance = pd.merge(major_category_variance, major_prod_cat, on='MAJOR_CAT_CD')
major_category_variance = major_category_variance.sort_values(by=['std'],ascending=False)

# Get all Major Category info for state by state
major_category_df = major_category_df.sort_values(by=['MAJOR_CAT_CD','SLS_AMT'],ascending=False)
major_category_df = pd.merge(major_category_df, major_prod_cat, on='MAJOR_CAT_CD')

# Output Relevant Data to csv files
report_folder = os.path.join(os.getcwd(), 'report_data')
state_report_folder = os.path.join(report_folder, 'state_reports')
major_category_leading.to_csv(os.path.join(state_report_folder,'major_category_leading_state.csv'))
category_leading.to_csv(os.path.join(state_report_folder,'category_leading_state.csv'))
prod_leading.to_csv(os.path.join(state_report_folder,'largest_product_largest_category_state.csv'))
state_average_df.to_csv(os.path.join(state_report_folder, 'max_min_state_sales_per_store.csv'))
zip_sales_max_df.to_csv(os.path.join(state_report_folder, 'max_zip_each_state.csv'))
zip_sales_min_df.to_csv(os.path.join(state_report_folder, 'min_zip_each_state.csv'))
major_category_variance.to_csv(os.path.join(state_report_folder,'major_category_variance_state.csv'))
# prod_leading_rev.to_csv(os.path.join(state_report_folder,'products_leading_state.csv'))
major_category_df.to_csv(os.path.join(state_report_folder,'major_category_state.csv'))

state_month_report_folder = os.path.join(report_folder, 'state_month_reports')
major_category_month_leading.to_csv(os.path.join(state_month_report_folder,'major_category_leading_state_month.csv'))
category_month_leading.to_csv(os.path.join(state_month_report_folder,'category_leading_state_month.csv'))
prod_month_leading.to_csv(os.path.join(state_month_report_folder,'prod_month_leading_category_state_month.csv'))
month_max_min_df.to_csv(os.path.join(state_month_report_folder, 'max_min_month_sales_by_state.csv'))
# prod_month_leading_rev.to_csv(os.path.join(state_month_report_folder, 'prod_leading_state_month.csv'))
state_month_variance.to_csv(os.path.join(state_month_report_folder, 'state_month_variance.csv'))

# Gather total sales by date
dates = pd.DataFrame(merged_df['SLS_DTE_NBR'].drop_duplicates())
dates['total_sales'] = [sum(merged_df[merged_df['SLS_DTE_NBR'] == date]['line_item_sale']) for date in dates['SLS_DTE_NBR']]
# Sort dates by total sales
dates = dates.sort_values(by=['total_sales'], ascending=False)

months = merged_df['month'].drop_duplicates()

month_category_df = pd.DataFrame([])
month_major_category_df = pd.DataFrame([])
month_total_df = pd.DataFrame([])
month_largest_major_cat_df = pd.DataFrame([])
month_largest_cat_df = pd.DataFrame([])
month_largest_prod_cat_df = pd.DataFrame([])

# month_largest_prod_cat_df_rev = pd.DataFrame([])

for month in months:
    month_df = merged_df[merged_df.month == month]

    # Calculate Major Category sales in month
    month_major_categories = pd.DataFrame(month_df['MAJOR_CAT_CD'].drop_duplicates())
    month_major_categories['SLS_QTY'] = [sum(month_df[month_df['MAJOR_CAT_CD'] == major_category]['SLS_QTY']) for major_category in month_major_categories['MAJOR_CAT_CD']]
    month_major_categories['SLS_AMT'] = [sum(month_df[month_df['MAJOR_CAT_CD'] == major_category]['line_item_sale']) for major_category in month_major_categories['MAJOR_CAT_CD']]
    month_major_categories['month'] = [month] * len(month_major_categories['MAJOR_CAT_CD'])
    month_major_category_df = pd.concat([month_major_category_df, month_major_categories])

    # Determine leading major category in sales each month
    month_largest_maj_cat = month_major_categories[
        month_major_categories['SLS_AMT'] == month_major_categories['SLS_AMT'].max()]
    month_largest_maj_cat = month_largest_maj_cat.astype({'MAJOR_CAT_CD': str})
    month_largest_maj_cat = pd.merge(month_largest_maj_cat, major_prod_cat, on='MAJOR_CAT_CD')
    month_largest_major_cat_df = pd.concat([month_largest_major_cat_df, month_largest_maj_cat])

    # Calculate Category sales in month
    month_categories = pd.DataFrame(month_df['CAT_CD'].drop_duplicates())
    month_categories['SLS_QTY'] = [sum(month_df[month_df['CAT_CD'] == category]['SLS_QTY']) for category in month_categories['CAT_CD']]
    month_categories['SLS_AMT'] = [sum(month_df[month_df['CAT_CD'] == category]['line_item_sale']) for category in month_categories['CAT_CD']]
    month_categories['month'] = [month] * len(month_categories['CAT_CD'])
    month_category_df = pd.concat([month_category_df, month_categories])

    # Determine leading  product in leading category in sales each month
    month_largest_cat = month_categories[
        month_categories['SLS_AMT'] == month_categories['SLS_AMT'].max()]
    month_largest_cat = month_largest_cat.astype({'CAT_CD': str})
    month_largest_cat = pd.merge(month_largest_cat, prod_cat, on='CAT_CD')
    month_largest_cat_df = pd.concat([month_largest_cat_df, month_largest_cat])

    # Calculate product sales in leading category in month
    product_month_leading_cat_df = month_df[month_df['CAT_CD'] == month_largest_cat['CAT_CD'].values[0]]
    month_products = pd.DataFrame(product_month_leading_cat_df['PROD_NBR'].drop_duplicates())
    month_products = month_products.astype({'PROD_NBR': str})
    month_products = pd.merge(month_products, prod_master, on='PROD_NBR')
    month_products['SLS_AMT'] = [sum(product_month_leading_cat_df[product_month_leading_cat_df['PROD_NBR'] == product]['line_item_sale']) for product in
                                   month_products['PROD_NBR']]
    month_products['month'] = [month] * len(month_products['PROD_NBR'])
    month_largest_cat_largest_prod = month_products[month_products['SLS_AMT']==month_products['SLS_AMT'].max()]
    month_largest_prod_cat_df = pd.concat([month_largest_prod_cat_df, month_largest_cat_largest_prod])

    # Determine product sales in month (NOTE: highly memory intensive)
    # month_products_rev = pd.DataFrame(month_df['PROD_NBR'].drop_duplicates())
    # month_products_rev = month_products_rev.astype({'PROD_NBR': str})
    # month_products_rev = pd.merge(month_products_rev, prod_master, on='PROD_NBR')
    # month_products_rev['SLS_AMT'] = [sum(month_df[month_df['PROD_NBR'] == product]['line_item_sale']) for product in
    #                                month_products_rev['PROD_NBR']]
    # month_products_rev['month'] = [month] * len(month_products_rev['PROD_NBR'])
    # month_largest_prod = month_products_rev[month_products_rev['SLS_AMT']==month_products_rev['SLS_AMT'].max()]
    # month_largest_prod_cat_df_rev = pd.concat([month_largest_prod_cat_df_rev, month_largest_prod])

    month_total = pd.DataFrame(month_df['month'].drop_duplicates())
    month_total['total_sales'] = sum(month_df['line_item_sale'])
    month_total_df = pd.concat([month_total_df, month_total])

# How do months vary
month_variance_describe = month_total_df['total_sales'].describe().tolist()
month_variance_list = []
month_variance_list.append(month_variance_describe)
month_variance = pd.DataFrame(month_variance_list, columns=['count','mean','std','min','25','50','75','max'])

# How do Major Product Categories vary from month to month
month_major_category_df = month_major_category_df.astype({'MAJOR_CAT_CD': str})
month_major_category_variance_list = []
month_major_categories = pd.DataFrame(month_major_category_df['MAJOR_CAT_CD'].drop_duplicates())
for major_category in month_major_categories['MAJOR_CAT_CD']:
    month_major_cat_df = month_major_category_df[month_major_category_df['MAJOR_CAT_CD'] == major_category]
    month_major_cat_variance = month_major_cat_df['SLS_AMT'].describe().tolist()
    month_major_cat_variance.insert(0,major_category)
    month_major_category_variance_list.append(month_major_cat_variance)
month_major_category_variance = pd.DataFrame(month_major_category_variance_list, columns=['MAJOR_CAT_CD','count','mean','std','min','25','50','75','max'])
month_major_category_variance = pd.merge(month_major_category_variance, major_prod_cat, on='MAJOR_CAT_CD')
month_major_category_variance = month_major_category_variance.sort_values(by=['std'],ascending=False)

# Get all Major Category info for month by month
month_major_category_df = month_major_category_df.sort_values(by=['MAJOR_CAT_CD','SLS_AMT'],ascending=False)
month_major_category_df = pd.merge(month_major_category_df, major_prod_cat, on='MAJOR_CAT_CD')


month_category_df = month_category_df.sort_values(by=['CAT_CD', 'SLS_AMT'], ascending=True)

month_total_df = month_total_df.sort_values(by=['total_sales'], ascending=False)

# Output Relevant Data to csv files
month_report_folder = os.path.join(report_folder, 'month_reports')
month_largest_major_cat_df.to_csv(os.path.join(month_report_folder, 'month_largest_major_cat.csv'))
month_largest_cat_df.to_csv(os.path.join(month_report_folder, 'month_largest_cat.csv'))
month_largest_prod_cat_df.to_csv(os.path.join(month_report_folder, 'month_largest_prod_cat.csv'))
month_major_category_variance.to_csv(os.path.join(month_report_folder, 'month_major_cat_variance.csv'))
month_total_df.to_csv(os.path.join(month_report_folder, 'month_total.csv'))
dates.to_csv(os.path.join(month_report_folder,'dates.csv'))
# month_largest_prod_cat_df_rev.to_csv(os.path.join(month_report_folder, 'month_largest_products.csv'))
month_variance.to_csv(os.path.join(month_report_folder,'month_variance.csv'))
month_major_category_df.to_csv(os.path.join(month_report_folder,'month_major_category.csv'))