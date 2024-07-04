import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Print the current working directory to confirm the path context
# print("Current Directory:", os.getcwd())

# Function to extract individual parent categories from the Parent Category Path
def extract_parents(parent_path):
    # Remove the "GS1//" prefix if it exists
    if parent_path.startswith("GS1//"):
        parent_path = parent_path[len("GS1//"):]
    # Split by "//" to get individual parent categories
    parts = parent_path.split('//')
    # Extract Segment, Family, and Class codes with safe indexing
    return (
        parts[0] if len(parts) > 0 else '',
        parts[1] if len(parts) > 1 else '',
        parts[2] if len(parts) > 2 else ''
    )

def create_category_df(categories, level, sheet):
    print(f'## Create categories at {level} level ##')

    # print(sheet)
    # exit()
    # Initialize an empty DataFrame for categories with specified columns
    columns = sheet.drop(columns=['Segment Code', 'Family Code', 'Class Code']).columns
    Category = pd.DataFrame(columns=columns)
    name_col = level.capitalize() + ' Code'
    path_cols = get_path_cols(level)
    
    # Assign sorted codes to the Category Name column
    Category['Category Name'] = sorted(categories)

    # Define locale settings based on the sheet
    if sheet.equals(maxeda_s9):
        locales = [{'title_prefix': '', 'locale': ''}]
    elif sheet.equals(maxeda_s10):
        locales = [
            {'title_prefix': 'NL ', 'locale': 'nl_NL'},
            {'title_prefix': 'FR ', 'locale': 'fr_FR'}
        ]

    # Process each locale setting
    all_categories = pd.DataFrame()
    for setting in locales:
        _Category = process_locale_setting(Category.copy(), setting, name_col, path_cols, level)
        all_categories = pd.concat([all_categories, _Category], ignore_index=True)
    
    if sheet.equals(maxeda_s10):
        # Duplicate for 'BE' locales
        be_categories = all_categories.copy()
        be_categories['Locale'] = be_categories['Locale'].replace({'nl_NL': 'nl_BE', 'fr_FR': 'fr_BE'})
        all_categories = pd.concat([all_categories, be_categories], ignore_index=True)

    return all_categories

def get_path_cols(level):
    """Define path columns based on category level."""
    if level == 'brick':
        return ['Segment Code', 'Family Code', 'Class Code']
    elif level == 'family':
        return ['Segment Code']
    elif level == 'class':
        return ['Segment Code', 'Family Code']
    return []

def process_locale_setting(Category, setting, name_col, path_cols, level):
    """Process a single locale setting and return the updated DataFrame."""
    title_col = setting['title_prefix'] + level.capitalize() + ' Title'
    if setting['locale']:
        Category['Locale'] = setting['locale']
    
    Category['Category Long Name'] = Category['Category Name'].map(
        lambda x: f"{x} - {gs1_df.loc[gs1_df[name_col].astype(str) == x, title_col].iloc[0]}"
        if x in gs1_df[name_col].astype(str).values else ''
    )
    
    Category['Parent Category Path'] = Category['Category Name'].apply(
        lambda x: f"GS1//{'//'.join(gs1_df.loc[gs1_df[name_col].astype(str) == x, path_cols].astype(str).iloc[0])}"
        if x in gs1_df[name_col].astype(str).values else 'GS1'
    )

    Category['Hierarchy Name'] = 'GS1 Hierarchy'

    # Duplicate for 'GPC' hierarchy
    Category_dup = Category.copy()
    Category_dup['Category Long Name'] = Category_dup['Category Long Name'].str.split(' - ').str[1]
    Category_dup['Parent Category Path'] = Category_dup['Parent Category Path'].str.replace('GS1//', '').str.replace('GS1', '')
    Category_dup['Hierarchy Name'] = 'GPC'

    return pd.concat([Category, Category_dup], ignore_index=True)

def delete_category_df(categories, sheet):
    # print(f'### Delete {sheet} ###')    
    # Filter the datamodel on the rows that need deletions
    Delete_categories = sheet[sheet['Category Name'].isin(categories)].copy()
    # Delete_categories = Delete_categories[['ID', 'Action', 'Unique Identifier', 'Category Name', 'Category Long Name', 'Parent Category Path', 'Hierarchy Name']]

    # Substitute nan by blank
    Delete_categories.replace('nan', '', inplace=True)

    # Add the delete action
    Delete_categories['Action'] = 'Delete'
    
    return Delete_categories

# Set file paths and read the specified sheets
gs1_file_path = os.getenv('path_datamodel_GS1')
datamodel_file_path = os.getenv('path_datamodel_maxeda')
file_path_workflowSKUs = os.getenv('file_path_workflowSKUs')

###################
## GS1 datamodel
###################
print('### Read GS1 datamodel ###')
# Read the 'Bricks' sheet from the GS1 file, starting from row 4 for headers
gs1_df = pd.read_excel(gs1_file_path, sheet_name='Bricks', skiprows=3, dtype=str)
# Select relevant columns
gs1_df = gs1_df[['Brick Code','Brick activated', 'Brick Title', 'Segment Code', 'Segment Title', 'Family Code', 'Family Title', 'Class Code', 'Class Title', 'FR Brick Title', 'FR Segment Title', 'FR Family Title', 'FR Class Title','NL Brick Title', 'NL Segment Title', 'NL Family Title', 'NL Class Title']].astype(str)

###################
## Maxeda datamodel
###################
print(f'### Read Maxeda datamodel ###')
def maxeda_sheet(sheet):
    print(f'## {sheet} ##')
    # Read the 'S9 - Category' sheet from the Datamodel file
    maxeda_sheet = pd.read_excel(datamodel_file_path, sheet_name=sheet, dtype=str)
    # Select relevant columns
    maxeda_sheet = maxeda_sheet.astype(str).apply(lambda x: x.str.strip("'\""))

    # Apply the function to extract Segment, Family, and Class Codes from the datamodel_bricks
    print('# Extract parents #')
    maxeda_sheet[['Segment Code', 'Family Code', 'Class Code']] = maxeda_sheet['Parent Category Path'].apply(extract_parents).apply(pd.Series)

    return maxeda_sheet

maxeda_s9 = maxeda_sheet('S9 - Category')
maxeda_s10 = maxeda_sheet('S10 - Category - Locale')

# Workflow
workflowSKUs_df = pd.read_excel(file_path_workflowSKUs, header=1)  # Header is in the second row

print('### Establish active bricks ###')
# Convert data to sets for comparison of just the Category Names
# print(gs1_df)
gs1_df_active= gs1_df[gs1_df['Brick activated'] == 'Yes']
gs1_active_brick_set = set(gs1_df_active['Brick Code'].dropna())
gs1_active_segment_set = set(gs1_df_active['Segment Code'].dropna())
gs1_active_family_set = set(gs1_df_active['Family Code'].dropna())
gs1_active_class_set = set(gs1_df_active['Class Code'].dropna())

def sheet(sheet_df):
    # Define the columns to keep
    if sheet_df.equals(maxeda_s9):
        columns_to_keep = ['ID', 'Action', 'Unique Identifier', 'Category Name', 'Category Long Name', 'Parent Category Path', 'Hierarchy Name']
        sheet_being_processes = 'S9'
    elif sheet_df.equals(maxeda_s10):
        columns_to_keep = ['ID', 'Action', 'Unique Identifier', 'Category Name', 'Parent Category Path', 'Hierarchy Name', 'Locale', 'Category Long Name']
        sheet_being_processes = 'S10'

    print(f'### Process {sheet_being_processes} ###')


    all_categories = []

    # Filter the DataFrame to include only rows where 'Hierarchy Name' is 'GS1 Hierarchy'
    base_fiter_datamodel = sheet_df[(sheet_df['Hierarchy Name'].isin(['GS1 Hierarchy']))]

    # Convert datamodel to sets
    filtered_datamodel_brick = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 3)]
    datamodel_brick_set = set(filtered_datamodel_brick['Category Name'].dropna())

    filtered_datamodel_segment = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 0)]
    datamodel_segment_set = set(filtered_datamodel_segment['Category Name'].dropna())

    filtered_datamodel_family = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 1)]
    datamodel_family_set = set(filtered_datamodel_family['Category Name'].dropna())

    filtered_datamodel_class = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 2)]
    datamodel_class_set = set(filtered_datamodel_class['Category Name'].dropna())

    #####################
    ## Workflow
    #####################
    print('## Process workflow SKUs ##')
    # Selecting required columns from the main DataFrame
    workflowSKUs_df_selected = workflowSKUs_df[['Brick', 'GTIN', 'ArticleLongName', 'VendorNumberSAP']]

    # Select only the columns needed for the merge from sheet
    sheet_relevant_columns = sheet_df[['Category Name', 'Segment Code', 'Family Code', 'Class Code']]

    # Merge with gs1_file_path_workflowSKUs_df to get the Segment Code, Family Code, and Class Code
    workflowSKUs_df_merged = pd.merge(workflowSKUs_df_selected, sheet_relevant_columns, how='left', left_on='Brick', right_on='Category Name')

    def workflow_aggregation(level):
        if level not in ['Brick', 'Segment Code', 'Family Code', 'Class Code']:
            raise ValueError("Invalid aggregation level")

        # Group by the specified level and aggregate
        result = workflowSKUs_df_merged.groupby(level).agg({
            'GTIN': 'nunique',  # Count of unique SKUs
            'VendorNumberSAP': 'nunique', # Unique Vendors
            'ArticleLongName': 'nunique'  # Unique count of ArticleLongName
        }).reset_index()  # Reset index to make the level a column again

        # Rename columns
        result.columns = ['Category', 'Unique_GTIN_Count', 'Unique_VendorNumberSAP', 'Unique_ArticleLongName_Count']

        # Add a new column to denote the level
        result['Level'] = level

        return result

    # Aggregate for each level
    brick_workflow_agg = workflow_aggregation('Brick')
    segment_workflow_agg = workflow_aggregation('Segment Code')
    family_workflow_agg = workflow_aggregation('Family Code')
    class_workflow_agg = workflow_aggregation('Class Code')

    # Print the resulting DataFrames
    # print("Brick Aggregation:")
    # print(brick_workflow_agg)
    # print("\nSegment Aggregation:")
    # print(segment_workflow_agg)
    # print("\nFamily Aggregation:")
    # print(family_workflow_agg)
    # print("\nClass Aggregation:")
    # print(class_workflow_agg)

    #####################
    ## Add
    #####################
    # Create a list to store data for new categories
    levels_and_data_new = [
        ('brick', gs1_active_brick_set - datamodel_brick_set),
        ('segment', gs1_active_segment_set - datamodel_segment_set),
        ('family', gs1_active_family_set - datamodel_family_set),
        ('class', gs1_active_class_set - datamodel_class_set)
    ]

    # Dictionary to hold individual DataFrames for each level
    individual_new_dfs = {}   

    # Loop through each level to generate corresponding DataFrames
    for level, selection in levels_and_data_new:
        new_categories = create_category_df(selection, level, sheet_df)
        new_categories['Reason'] = 'add: ' + level
        new_categories = new_categories[columns_to_keep]
        all_categories.append(new_categories)

        # Store the individual DataFrame in the dictionary
        individual_new_dfs[level] = new_categories

    # Extract individual DataFrames for each level
    new_brick_categories_df = individual_new_dfs.get('brick')
    new_segment_categories_df = individual_new_dfs.get('segment')
    new_family_categories_df = individual_new_dfs.get('family')
    new_class_categories_df = individual_new_dfs.get('class')

    #####################
    ## Delete
    #####################
    print('## Deletions ##')
    # We only delete bricks if they are not in the model at all NOT ONLY looking at active bricks
    gs1_brick_set = set(gs1_df['Brick Code'].dropna())
    gs1_segment_set = set(gs1_df['Segment Code'].dropna())
    gs1_family_set = set(gs1_df['Family Code'].dropna())
    gs1_class_set = set(gs1_df['Class Code'].dropna())

        #####################
        ## Integrate backlog
        #####################
    
    # Configure brick deletions
    initial_brick_deletion_set = datamodel_brick_set - gs1_brick_set
    brick_workflow_set = set(brick_workflow_agg['Category'].dropna())
    final_brick_deletion_set = initial_brick_deletion_set - brick_workflow_set
    backlog_brick_deletion_set = initial_brick_deletion_set & brick_workflow_set

    # Filter brick_workflow_agg to exclude Brick Codes in final_brick_deletion_set
    backlog_brick_deletions = brick_workflow_agg[brick_workflow_agg['Category'].isin(backlog_brick_deletion_set)].copy()
    if not backlog_brick_deletions.empty:
        backlog_brick_deletions.loc[:, 'Reason_backlog'] = 'Brick deletion' 

    # Configure segments deletions
    initial_segment_deletion_set = datamodel_segment_set - gs1_segment_set
    segment_workflow_set = set(segment_workflow_agg['Category'].dropna())
    final_segment_deletion_set = initial_segment_deletion_set - segment_workflow_set
    backlog_segment_deletion_set = initial_segment_deletion_set & segment_workflow_set

    # Filter segment_workflow_agg to exclude segment Codes in final_segment_deletion_set
    backlog_segment_deletions = segment_workflow_agg[segment_workflow_agg['Category'].isin(backlog_segment_deletion_set)].copy()
    if not backlog_segment_deletions.empty:
        backlog_segment_deletions.loc[:, 'Reason_backlog'] = 'Segment deletion' 

    # Configure family deletions
    initial_family_deletion_set = datamodel_family_set - gs1_family_set
    family_workflow_set = set(family_workflow_agg['Category'].dropna())
    final_family_deletion_set = initial_family_deletion_set - family_workflow_set
    backlog_family_deletion_set = initial_family_deletion_set & family_workflow_set

    # Filter family_workflow_agg to exclude family Codes in final_family_deletion_set
    backlog_family_deletions = family_workflow_agg[family_workflow_agg['Category'].isin(backlog_family_deletion_set)].copy()
    if not backlog_family_deletions.empty:
        backlog_family_deletions.loc[:, 'Reason_backlog'] = 'Family deletion' 

    # Configure class deletions
    initial_class_deletion_set = datamodel_class_set - gs1_class_set
    class_workflow_set = set(class_workflow_agg['Category'].dropna())
    final_class_deletion_set = initial_class_deletion_set - class_workflow_set
    backlog_class_deletion_set = initial_class_deletion_set & class_workflow_set

    # Filter class_workflow_agg to exclude class Codes in final_class_deletion_set
    backlog_class_deletions = class_workflow_agg[class_workflow_agg['Category'].isin(backlog_class_deletion_set)].copy()
    if not backlog_class_deletions.empty:
        backlog_class_deletions.loc[:, 'Reason_backlog'] = 'Class deletion' 

        #####################
        ## 
        #####################

    # print(f"initial_brick_deletion_set: {initial_brick_deletion_set}")
    # print(f"workflow_brick_set: {set(brick_workflow_agg['Brick'])}")
    # print(f"backlog_brick_deletion_set: {backlog_brick_deletion_set}")
    # print(f"backlog_brick_deletions: {backlog_brick_deletions}")
    # exit()


    # Create a list to store data for new categories
    levels_and_data_delete = [
        ('brick', final_brick_deletion_set),
        ('segment', final_segment_deletion_set),
        ('family', final_family_deletion_set),
        ('class', final_class_deletion_set)
    ]

    # Dictionary to hold individual DataFrames for each level
    individual_delete_dfs = {}
    
    # Loop through the levels and update the combined set with each difference set
    for level, difference_set in levels_and_data_delete:
        delete_categories = delete_category_df(difference_set, sheet_df)
        # Filter out rows where 'Category Name' starts with '999'
        delete_categories = delete_categories[~delete_categories['Category Name'].str.startswith('999')]
        delete_categories['Reason'] = 'Delete'
        delete_categories = delete_categories[columns_to_keep]
        all_categories.append(delete_categories)

        # Store the individual DataFrame in the dictionary
        individual_delete_dfs[level] = delete_categories

    # Extract individual DataFrames for each level
    delete_brick_categories_df = individual_delete_dfs.get('brick')
    delete_segment_categories_df = individual_delete_dfs.get('segment')
    delete_family_categories_df = individual_delete_dfs.get('family')
    delete_class_categories_df = individual_delete_dfs.get('class')

    ######################
    ## Change in hierachy of active bricks
    #####################
    print('## Hierarchy change ##')

    def merge_and_check_hierarchy(gs1_df, sheet_df, merge_columns, check_columns, hierarchy_name):
        # Merge the two dataframes on the specified columns
        left_on, right_on = merge_columns
        comparison_df = pd.merge(gs1_df, sheet_df, how='inner', left_on=left_on, right_on=right_on)

        # Check for mismatched parents using the provided columns
        condition = False
        for col_x, col_y in check_columns:
            condition |= (comparison_df[f'{col_x}_x'] != comparison_df[f'{col_y}_y'])

        change_hierarchy_df = comparison_df[condition].copy()

        # Create the new 'Parent Category Path' based on the given conditions
        change_hierarchy_df['Parent Category Path'] = change_hierarchy_df.apply(
            lambda row: f"GS1//{row[f'{check_columns[0][0]}_x']}//{'//'.join([row[f'{col[0]}_x'] for col in check_columns[1:]])}" 
            if row['Hierarchy Name'] == hierarchy_name
            else f"{row[f'{check_columns[0][0]}_x']}//{'//'.join([row[f'{col[0]}_x'] for col in check_columns[1:]])}",
            axis=1
        )

        # Replace 'nan' strings with an empty string in specific columns
        change_hierarchy_df['Action'] = change_hierarchy_df['Action'].replace('nan', '')
        change_hierarchy_df['Unique Identifier'] = change_hierarchy_df['Unique Identifier'].replace('nan', '')


        change_hierarchy_df = change_hierarchy_df[columns_to_keep]
        
        return change_hierarchy_df

    # Specify the hierarchy name
    hierarchy_name = 'GS1 Hierarchy'

    # Define the check columns for each level
    brick_check_columns = [('Segment Code', 'Segment Code'), ('Family Code', 'Family Code'), ('Class Code', 'Class Code')]
    class_check_columns = [('Segment Code', 'Segment Code'), ('Family Code', 'Family Code')]
    family_check_columns = [('Segment Code', 'Segment Code')]

    # Process each hierarchy level
    # For bricks, the merge columns are different
    change_hierarchy_brick_df = merge_and_check_hierarchy(gs1_df_active, sheet_df, ('Brick Code', 'Category Name'), brick_check_columns, hierarchy_name)
    change_hierarchy_class_df = merge_and_check_hierarchy(gs1_df_active, sheet_df, ('Class Code', 'Category Name'), class_check_columns, hierarchy_name)
    change_hierarchy_family_df = merge_and_check_hierarchy(gs1_df_active, sheet_df, ('Family Code', 'Category Name'), family_check_columns, hierarchy_name)

    # print(change_hierarchy_brick_df)
    # exit()
        #####################
        ## Integrate backlog
        #####################
    
    # Configure brick deletions
    initial_brick_change_set = set(change_hierarchy_brick_df['Category Name'].dropna())
    final_brick_change_set = initial_brick_change_set - brick_workflow_set
    backlog_brick_change_set = initial_brick_change_set & brick_workflow_set

    # Filter brick_workflow_agg to exclude Brick Codes in final_brick_deletion_set
    backlog_brick_changes = brick_workflow_agg[brick_workflow_agg['Category'].isin(backlog_brick_change_set)].copy()
    if not backlog_brick_changes.empty:
        backlog_brick_changes.loc[:, 'Reason_backlog'] = 'Brick hierarchy change'

    # Configure family deletions
    initial_family_change_set = set(change_hierarchy_family_df['Category Name'].dropna())
    final_family_change_set = initial_family_change_set - family_workflow_set
    backlog_family_change_set = initial_family_change_set & family_workflow_set

    # Filter family_workflow_agg to exclude family Codes in final_family_deletion_set
    backlog_family_changes = family_workflow_agg[family_workflow_agg['Category'].isin(backlog_family_change_set)].copy()
    if not backlog_family_changes.empty:
        backlog_family_changes.loc[:, 'Reason_backlog'] = 'Family hierarchy change'
    
    # Configure class deletions
    initial_class_change_set = set(change_hierarchy_class_df['Category Name'].dropna())
    final_class_change_set = initial_class_change_set - class_workflow_set
    backlog_class_change_set = initial_class_change_set & class_workflow_set

    # Filter class_workflow_agg to exclude class Codes in final_class_deletion_set
    backlog_class_changes = class_workflow_agg[class_workflow_agg['Category'].isin(backlog_class_change_set)].copy()
    if not backlog_class_changes.empty:
        backlog_class_changes.loc[:, 'Reason_backlog'] = 'Class hierarchy change' 

    # List of DataFrames to concatenate
    backlog_dfs = [backlog_brick_deletions, backlog_segment_deletions, backlog_family_deletions, backlog_class_deletions, backlog_brick_changes, backlog_family_changes, backlog_class_changes]

    # Concatenate all DataFrames into a single DataFrame
    backlog_df = pd.concat(backlog_dfs, ignore_index=True)


        #####################
        ## 
        #####################

    change_hierarchy_brick_df = change_hierarchy_brick_df[change_hierarchy_brick_df['Category Name'].isin(final_brick_change_set)]
    change_hierarchy_class_df = change_hierarchy_class_df[change_hierarchy_class_df['Category Name'].isin(final_class_change_set)]
    change_hierarchy_family_df = change_hierarchy_family_df[change_hierarchy_family_df['Category Name'].isin(final_family_change_set)]

    # change_hierarchy_brick_df['Reason'] = 'Change: Brick hierarchy'
    # change_hierarchy_class_df['Reason'] = 'Change: Class hierarchy'
    # change_hierarchy_family_df['Reason'] = 'Change: Family hierarchy'

    # Append the results to the all_categories list
    all_categories.append(change_hierarchy_brick_df)
    all_categories.append(change_hierarchy_class_df)
    all_categories.append(change_hierarchy_family_df)

################################################

    # Concatenate all DataFrames into one, if there are any DataFrames to concatenate
    if all_categories:
        final_all_categories = pd.concat(all_categories, ignore_index=True)
    else:
        final_all_categories = pd.DataFrame()  # Fallback to an empty DataFrame if no data

    return final_all_categories, delete_brick_categories_df, delete_class_categories_df, delete_family_categories_df, delete_segment_categories_df, new_brick_categories_df, new_class_categories_df, new_family_categories_df, new_segment_categories_df, change_hierarchy_brick_df, change_hierarchy_class_df, change_hierarchy_family_df, backlog_df



final_all_categories_s9, delete_brick_categories_s9, delete_class_categories_s9, delete_family_categories_s9, delete_segment_categories_s9, new_brick_categories_s9, new_class_categories_s9, new_family_categories_s9, new_segment_categories_s9, change_hierarchy_bricks_s9, change_hierarchy_classes_s9, change_hierarchy_families_s9, backlog_S9_df = sheet(maxeda_s9)
final_all_categories_locale_combined_s10, delete_brick_categories_s10, delete_class_categories_s10, delete_family_categories_s10, delete_segment_categories_s10, new_brick_categories_s10, new_class_categories_s10, new_family_categories_s10, new_segment_categories_s10, change_hierarchy_bricks_s10, change_hierarchy_classes_s10, change_hierarchy_families_s10, backlog_S10_df = sheet(maxeda_s10)

final_backlog_df = pd.concat([backlog_S9_df, backlog_S10_df], ignore_index=True)
final_backlog_df = final_backlog_df.drop_duplicates()

###################
## Write output total
###################
print('### Output ###')

# print(final_all_categories_locale_combined)

# Write the concatenated DataFrame to an Excel file
output_file_path = os.path.join(os.getcwd(), 'GS1_vs_Datamodel_Comparison_Bricks.xlsx')

metadata_df = pd.DataFrame({
    'Sheet No': ['S9', 'S10'],
    'DataModel Type Name': ['Category', 'Category - Localized'],
    'Physical Sheet Name': ['S9 - Category', 'S10 - Category Locale'],
    'Load Lookup?': ['NO', 'NO']
})

with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
    final_all_categories_s9.to_excel(writer, sheet_name='S9 - Category', index=False)
    final_all_categories_locale_combined_s10.to_excel(writer, sheet_name='S10 - Category - Locale', index=False)
            
# Load the updated Excel file into a DataFrame
# comparison_s9 = pd.read_excel(output_file_path, sheet_name='S9 - Category')
# print(len(comparison_s9))
   
# comparison_s10 = pd.read_excel(output_file_path, sheet_name='S10 - Category - Locale')
# print(len(comparison_s10))

############################
## Backlog
############################

# Write the concatenated DataFrame to an Excel file
output_file_path = os.path.join(os.getcwd(), 'X_Backlog.xlsx')

with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    final_backlog_df.to_excel(writer, sheet_name='Backlog', index=False)

############################
## Output in workflow
############################


# Define a list of DataFrames to iterate over
dataframes_s9 = {
    'new_segment_categories_S9AndS10': new_segment_categories_s9,
    'new_family_categories_S9AndS10': new_family_categories_s9,
    'new_class_categories_S9AndS10': new_class_categories_s9,
    'new_brick_categories_S9AndS10': new_brick_categories_s9,
    'delete_brick_categories_S9_autotriggers10': delete_brick_categories_s9,
    'delete_class_categories_S9_autotriggers10': delete_class_categories_s9,
    'delete_family_categories_S9_autotriggers10': delete_family_categories_s9,
    'delete_segment_categories_S9_autotriggers10': delete_segment_categories_s9,
}

dataframes_s10 = {
    'new_segment_categories_S9AndS10': new_segment_categories_s10,
    'new_family_categories_S9AndS10': new_family_categories_s10,
    'new_class_categories_S9AndS10': new_class_categories_s10,
    'new_brick_categories_S9AndS10': new_brick_categories_s10,
}

# Define counters for numbering the files
new_file_counter = 1
delete_file_counter = 12

# Helper function to extract metadata information
def extract_metadata(sheet_name):
    parts = sheet_name.split('-')
    sheet_no = parts[0].strip()
    data_model_type_name = parts[1].strip() if len(parts) > 1 else ''
    return sheet_no, data_model_type_name

# Save each DataFrame to a separate Excel file with a prefixed number
for name, dataframe in dataframes_s9.items():
    if name.startswith('new_'):
        output_file_path = os.path.join(os.getcwd(), f"{new_file_counter}_{name}.xlsx")
        new_file_counter += 1
    elif name.startswith('delete_'):
        output_file_path = os.path.join(os.getcwd(), f"{delete_file_counter}_{name}.xlsx")
        delete_file_counter += 1
    else:
        continue  # Skip if the name does not match the expected prefixes

    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        metadata = []

        # Prepare metadata
        sheet_name_s9 = 'S9 - Category'
        sheet_no, data_model_type_name = extract_metadata(sheet_name_s9)
        metadata.append([sheet_no, data_model_type_name, sheet_name_s9, 'NO'])

        if name in dataframes_s10:
            sheet_name_s10 = 'S10 - Category - Locale'
            sheet_no, data_model_type_name = extract_metadata(sheet_name_s10)
            metadata.append([sheet_no, data_model_type_name, sheet_name_s10, 'NO'])

        # Write Metadata sheet first
        metadata_df = pd.DataFrame(metadata, columns=['Sheet No', 'DataModel Type Name', 'Physical Sheet Name', 'Load Lookup?'])
        metadata_df.to_excel(writer, sheet_name='Metadata', index=False)

        # Write S9 sheet
        dataframe.to_excel(writer, sheet_name=sheet_name_s9, index=False)

        # Write S10 sheet if it exists
        if name in dataframes_s10:
            dataframes_s10[name].to_excel(writer, sheet_name=sheet_name_s10, index=False)

# Combine change_hierarchy DataFrames into one DataFrame
change_hierarchy_combined_df = pd.concat([change_hierarchy_bricks_s9, change_hierarchy_classes_s9, change_hierarchy_families_s9], ignore_index=True)

metadata_df = pd.DataFrame({
    'Sheet No': ['S9'],
    'DataModel Type Name': ['Category'],
    'Physical Sheet Name': ['S9 - Category'],
    'Load Lookup?': ['NO']
})


# Save the combined change_hierarchy DataFrame to a single Excel file
change_hierarchy_output_file_path = "5_change_hierarchy_combined.xlsx"
with pd.ExcelWriter(change_hierarchy_output_file_path, engine='openpyxl') as writer:
    metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
    change_hierarchy_combined_df.to_excel(writer, sheet_name='S9 - Category', index=False)

