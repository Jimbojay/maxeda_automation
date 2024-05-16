import pandas as pd
import os

# Print the current working directory to confirm the path context
print("Current Directory:", os.getcwd())

# Set file paths and read the specified sheets
gs1_file_path = 'Workfiles/GS1 datamodel 3.1.27 API.xlsx'
datamodel_file_path = 'Workfiles/Archief/DataModel_Template_20230920.xlsx'

###################
## GS1 datamodel
###################
print('### Read GS1 datamodel ###')
# Read the 'Bricks' sheet from the GS1 file, starting from row 4 for headers
gs1_df = pd.read_excel(gs1_file_path, sheet_name='Bricks', skiprows=3)

gs1_df = gs1_df[['Brick Code','Brick activated', 'Brick Title', 'Segment Code', 'Segment Title', 'Family Code', 'Family Title', 'Class Code', 'Class Title', 'FR Brick Title', 'FR Segment Title', 'FR Family Title', 'FR Class Title','NL Brick Title', 'NL Segment Title', 'NL Family Title', 'NL Class Title']].astype(str)
# gs1_df = gs1_df.applymap(lambda x: x.strip().upper())  # Clean and unify case


###################
## Maxeda datamodel
###################
print('### Read Maxeda datamodel ###')
# Read the 'S9 - Category' sheet from the Datamodel file
datamodel_df = pd.read_excel(datamodel_file_path, sheet_name='S9 - Category')

# Include 'Parent Category Path' in your analysis
datamodel_df = datamodel_df[['ID','Action','Unique Identifier','Category Name', 'Category Long Name', 'Parent Category Path','Hierarchy Name']].astype(str).applymap(lambda x: x.strip("'\""))
# datamodel_df = datamodel_df.applymap(lambda x: x.strip("'\"").strip().upper())  # Clean and unify case


# Filter rows based on the count of slashes in 'Parent Category Path'
# datamodel_bricks = datamodel_bricks[datamodel_bricks['Parent Category Path'].str.count('/') == 6]

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

# Apply the function to extract Segment, Family, and Class Codes from the datamodel_bricks
print('### Extract parents ###')
datamodel_df[['Segment Code', 'Family Code', 'Class Code']] = datamodel_df['Parent Category Path'].apply(extract_parents).apply(pd.Series)

# Function to create and duplicate categories for the specified brick codes
def create_category_df(categories, level):
    print(f'### Create categories at {level} level ###')
    # Initialize an empty DataFrame for categories
    Category = pd.DataFrame(columns=datamodel_df.drop(columns=['Segment Code', 'Family Code', 'Class Code']).columns)

    # Assign sorted codes to the Category Name column
    Category['Category Name'] = sorted(categories)
    
    # Set mapping behavior for each level
    if level == 'brick':
        name_col, path_cols = 'Brick Code', ['Segment Code', 'Family Code', 'Class Code']
    elif level == 'segment':
        name_col, path_cols = 'Segment Code', []
    elif level == 'family':
        name_col, path_cols = 'Family Code', ['Segment Code']
    elif level == 'class':
        name_col, path_cols = 'Class Code', ['Segment Code', 'Family Code']

    # Create category long names and parent paths based on levels
    if level == 'brick':
        Category['Category Long Name'] = Category['Category Name'].map(lambda x: x + ' - ' + gs1_df.loc[gs1_df[name_col].astype(str) == x, 'Brick Title'].iloc[0] if x in gs1_df[name_col].astype(str).values else '')
    else:
        title_col = level.capitalize() + ' Title'
        Category['Category Long Name'] = Category['Category Name'].map(lambda x: x + ' - ' + gs1_df.loc[gs1_df[name_col].astype(str) == x, title_col].iloc[0] if x in gs1_df[name_col].astype(str).values else '')

    # Handle parent category paths
    if path_cols:
        Category['Parent Category Path'] = Category['Category Name'].map(lambda x: 'GS1//' + '//'.join(gs1_df.loc[gs1_df[name_col].astype(str) == x, path_cols].astype(str).iloc[0]) if x in gs1_df[name_col].astype(str).values else '')
    else:
        Category['Parent Category Path'] = 'GS1'
    
    Category['Hierarchy Name'] = 'GS1 Hierarchy'
    
    # Duplicate for 'GPC' hierarchy
    Category_dup = Category.copy()
    Category_dup['Category Long Name'] = Category_dup['Category Long Name'].str.split(' - ').str[1]
    Category_dup['Parent Category Path'] = Category_dup['Parent Category Path'].str.replace('GS1//', '')
    Category_dup['Parent Category Path'] = Category_dup['Parent Category Path'].str.replace('GS1', '')
    Category_dup['Hierarchy Name'] = 'GPC'
    
    # print('dub')
    # print(Category_dup['Category Name'])
    # exit()

    # action_text = f"add {level}"
    # Category['Origin'] = action_text
    # Category_dup['Origin'] = action_text + " GPC"  # Differentiate GPC entries
    
    return pd.concat([Category, Category_dup], ignore_index=True)

def delete_category_df(categories):
    print('### Delete categories ###')    
    # Filter the datamodel on the rows that need deletions
    Delete_categories = datamodel_df[datamodel_df['Category Name'].isin(categories)]
    Delete_categories = Delete_categories[['ID', 'Action', 'Unique Identifier', 'Category Name', 'Category Long Name', 'Parent Category Path', 'Hierarchy Name']]

    # Substitute nan by blank
    Delete_categories.replace('nan', '', inplace=True)

    # Add the delete action
    Delete_categories['Action'] = 'Delete'
    
    # Apply function to determine the level based on 'Parent Category Path'
    def get_level_delete(parent_path):
        slash_count = parent_path.count('//')
        if slash_count == 2:
            return 'brick'
        elif slash_count == 1:
            return 'class'
        elif slash_count == 0:
            return 'family'
        elif slash_count == 0:
            return 'segment'
        return 'unknown'  # Default case if none match

    # Create a new column 'Level' based on the slash count
    # Delete_categories['Origin'] = Delete_categories['Parent Category Path'].apply(get_level_delete).apply(lambda x: f"delete {x}")

    
    return Delete_categories

#####################
## Bricks
#####################

# Create a new DataFrame using the structure of 'S9 - Category'
print('### Construct S9-Categories ###')

Category = pd.DataFrame(columns=datamodel_df.columns)
all_categories_s9 = []

    #####################
    ## Add
    #####################
print('### Additions ###')
# Convert data to sets for comparison of just the Category Names
# print(gs1_df)
gs1_df_active= gs1_df[gs1_df['Brick activated'] == 'Yes']
gs1_active_bricks_set = set(gs1_df_active['Brick Code'].dropna())
gs1_active_segments_set = set(gs1_df_active['Segment Code'].dropna())
gs1_active_families_set = set(gs1_df_active['Family Code'].dropna())
gs1_active_classes_set = set(gs1_df_active['Class Code'].dropna())

# Filter the DataFrame to include only rows where 'Hierarchy Name' is 'GS1 Hierarchy'
base_fiter_datamodel = datamodel_df[(datamodel_df['Hierarchy Name'].isin(['GS1 Hierarchy']))]

# Convert datamodel to sets
filtered_datamodel_bricks = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 3)]
datamodel_bricks_set = set(filtered_datamodel_bricks['Category Name'].dropna())

filtered_datamodel_segments = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 0)]
datamodel_segments_set = set(filtered_datamodel_segments['Category Name'].dropna())

filtered_datamodel_families = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 1)]
datamodel_families_set = set(filtered_datamodel_families['Category Name'].dropna())

filtered_datamodel_classes = base_fiter_datamodel[(base_fiter_datamodel['Parent Category Path'].str.count('//') == 2)]
datamodel_classes_set = set(filtered_datamodel_classes['Category Name'].dropna())

# print("filtered")
# print(filtered_datamodel_segments['Parent Category Path'])
# print(datamodel_segments_set)

# Create a list to store data for new categories
levels_and_data_new = [
    ('brick', gs1_active_bricks_set - datamodel_bricks_set),
    ('segment', gs1_active_segments_set - datamodel_segments_set),
    ('family', gs1_active_families_set - datamodel_families_set),
    ('class', gs1_active_classes_set - datamodel_classes_set)
]

# Loop through each level to generate corresponding DataFrames
for level, data in levels_and_data_new:
    new_categories = create_category_df(data, level)
    all_categories_s9.append(new_categories)

    #####################
    ## Delete
    #####################
print('### Deletions ###')
gs1_bricks_set = set(gs1_df['Brick Code'].dropna())
gs1_segments_set = set(gs1_df['Segment Code'].dropna())
gs1_families_set = set(gs1_df['Family Code'].dropna())
gs1_classes_set = set(gs1_df['Class Code'].dropna())

# Initialize an empty set to hold the combined results
delete_set = set()

# Create a list to store data for new categories
levels_and_data_delete = [
    ('brick', datamodel_bricks_set - gs1_bricks_set),
    ('segment', datamodel_segments_set - gs1_segments_set),
    ('family', datamodel_families_set - gs1_families_set),
    ('class', datamodel_classes_set - gs1_classes_set)
]

# print("segments")
# print(len(datamodel_families_set - gs1_families_set))
# print("maxeda seg")
# print(datamodel_bricks_set)
# print("gs1 seg")
# print(gs1_bricks_set)

# Loop through the levels and update the combined set with each difference set
for level, difference_set in levels_and_data_delete:
    delete_set.update(difference_set)

# print("length dif")
# print(len(datamodel_bricks_set - gs1_bricks_set))
# print(len(datamodel_segments_set - gs1_segments_set))
# print(len(datamodel_families_set - gs1_families_set))
# print(len(datamodel_classes_set - gs1_classes_set))
# print(len(delete_set))
delete_categories = delete_category_df(delete_set)
# Filter out rows where 'Category Name' starts with '999'
delete_categories = delete_categories[~delete_categories['Category Name'].str.startswith('999')]

# print(len(delete_categories))
# print("delete cat")
# print(datamodel_families_set - gs1_families_set)
# print("delete ca2")
# print(datamodel_bricks_set - gs1_bricks_set)

all_categories_s9.append(delete_categories)

    ######################
    ## Change in hierachy of active bricks
    #####################
print('### Hierarchy change ###')
# Merge the two dataframes on Brick Code
comparison_df_hierarchy = pd.merge(gs1_df_active, datamodel_df, how='inner', left_on='Brick Code', right_on='Category Name')

# Check for mismatched parents using trimmed and type-consistent comparisons
change_hierarchy_df = comparison_df_hierarchy[
    (comparison_df_hierarchy['Segment Code_x'] != comparison_df_hierarchy['Segment Code_y']) |
    (comparison_df_hierarchy['Family Code_x'] != comparison_df_hierarchy['Family Code_y']) |
    (comparison_df_hierarchy['Class Code_x'] != comparison_df_hierarchy['Class Code_y'])
]

change_hierarchy_set = set(change_hierarchy_df['Brick Code'].dropna())

# print('change hierarchy')
# print(change_hierarchy_set)

delete_old_hierarchy = delete_category_df(change_hierarchy_set)
create_new_hierarchy = create_category_df(change_hierarchy_set, 'brick')

all_categories_s9.append(delete_old_hierarchy)
all_categories_s9.append(create_new_hierarchy)

# Concatenate all DataFrames into one for S9 - Category, if there are any DataFrames to concatenate
if all_categories_s9:
    final_all_categories_s9 = pd.concat(all_categories_s9, ignore_index=True)
else:
    final_all_categories_s9 = pd.DataFrame()  # Fallback to an empty DataFrame if no data

###################
## S10 - Category - Locale
###################

print('### Construct S10-categories ###')

    ###################
    ## Datamodel dictionaries 
    ###################
print('### Dictonaries ###')

# Creating an empty dictionary to hold all mappings for translation into French
code_to_description_dict_fr = {}
code_to_description_dict_nl = {}

datamodel_df_s10_dict = pd.read_excel(datamodel_file_path, sheet_name='S10 - Category - Locale')
datamodel_df_s10_dict = datamodel_df_s10_dict[['Category Name', 'Locale', 'Category Long Name']].astype(str).applymap(lambda x: x.strip("'\""))


# Transform 'Category Long Name' to take the part after "- "
datamodel_df_s10_dict.loc[:, 'Category Long Name'] = datamodel_df_s10_dict['Category Long Name'].apply(
    lambda x: x.split("- ", 1)[1] if "- " in x else x
)

# Filter the DataFrame for rows where Locale is 'fr_FR'
datamodel_df_dict_fr = datamodel_df_s10_dict[datamodel_df_s10_dict['Locale'] == 'fr_FR'].copy()
# Update the dictionary using a Series created from the DataFrame
code_to_description_dict_fr.update(pd.Series(datamodel_df_dict_fr['Category Long Name'].values, index=datamodel_df_dict_fr['Category Name']).to_dict())

# Filter the DataFrame for rows where Locale is 'fr_FR'
datamodel_df_dict_nl = datamodel_df_s10_dict[datamodel_df_s10_dict['Locale'] == 'nl_NL'].copy()
# Update the dictionary using a Series created from the DataFrame
code_to_description_dict_nl.update(pd.Series(datamodel_df_dict_nl['Category Long Name'].values, index=datamodel_df_dict_nl['Category Name']).to_dict())


    ###################
    ## GS1 dictionary
    ###################


# Assuming specific pairs of code and description columns
# For example, 'Brick Code' and 'FR Brick Title', 'Segment Code' and 'FR Segment Title', etc.
# Add each pair to the dictionary
code_to_description_dict_fr.update(pd.Series(gs1_df['FR Brick Title'].values, index=gs1_df['Brick Code']).to_dict())
code_to_description_dict_fr.update(pd.Series(gs1_df['FR Segment Title'].values, index=gs1_df['Segment Code']).to_dict())
code_to_description_dict_fr.update(pd.Series(gs1_df['FR Family Title'].values, index=gs1_df['Family Code']).to_dict())
code_to_description_dict_fr.update(pd.Series(gs1_df['FR Class Title'].values, index=gs1_df['Class Code']).to_dict())

code_to_description_dict_nl.update(pd.Series(gs1_df['NL Brick Title'].values, index=gs1_df['Brick Code']).to_dict())
code_to_description_dict_nl.update(pd.Series(gs1_df['NL Segment Title'].values, index=gs1_df['Segment Code']).to_dict())
code_to_description_dict_nl.update(pd.Series(gs1_df['NL Family Title'].values, index=gs1_df['Family Code']).to_dict())
code_to_description_dict_nl.update(pd.Series(gs1_df['NL Class Title'].values, index=gs1_df['Class Code']).to_dict())

    ##################
    # Dataset
    ##################
print('### Dataset ###')    
# Copy the S9 - Category to S10 - Category - Locale for fr_FR
all_categories_s10_fr_FR = final_all_categories_s9.copy()
all_categories_s10_fr_FR['Locale'] = 'fr_FR'

print(all_categories_s10_fr_FR)

# Map the descriptions to the 'Category Name' column using the combined dictionary
all_categories_s10_fr_FR['Mapped Description'] = all_categories_s10_fr_FR['Category Name'].map(code_to_description_dict_fr)
# Construct the new field with "[Category Name] - [Description]"
all_categories_s10_fr_FR['New Category Long Name'] = all_categories_s10_fr_FR['Category Name'] + ' - ' + all_categories_s10_fr_FR['Mapped Description']
# Replace the values in the original 'Category Long Name' with the values from 'New Category Long Name'
all_categories_s10_fr_FR['Category Long Name'] = all_categories_s10_fr_FR['New Category Long Name']
# Drop the 'New Category Long Name' column as it's no longer needed
all_categories_s10_fr_FR.drop('New Category Long Name', axis=1, inplace=True)
# Drop the 'Mapped Description' column as it's no longer needed
all_categories_s10_fr_FR.drop('Mapped Description', axis=1, inplace=True)

all_categories_s10_nl_NL = final_all_categories_s9.copy()
all_categories_s10_nl_NL['Locale'] = 'nl_NL'

print(all_categories_s10_nl_NL)

# Map the descriptions to the 'Category Name' column using the combined dictionary
all_categories_s10_nl_NL['Mapped Description'] = all_categories_s10_nl_NL['Category Name'].map(code_to_description_dict_fr)
# Construct the new field with "[Category Name] - [Description]"
all_categories_s10_nl_NL['New Category Long Name'] = all_categories_s10_nl_NL['Category Name'] + ' - ' + all_categories_s10_nl_NL['Mapped Description']
# Replace the values in the original 'Category Long Name' with the values from 'New Category Long Name'
all_categories_s10_nl_NL['Category Long Name'] = all_categories_s10_nl_NL['New Category Long Name']
# Drop the 'New Category Long Name' column as it's no longer needed
all_categories_s10_nl_NL.drop('New Category Long Name', axis=1, inplace=True)
# Drop the 'Mapped Description' column as it's no longer needed
all_categories_s10_nl_NL.drop('Mapped Description', axis=1, inplace=True)

# Duplicate to accomodate for nl_BE and fr_BE
final_all_categories_locale_nl_BE = all_categories_s10_nl_NL.copy()
final_all_categories_locale_nl_BE['Locale'] = 'nl_BE'

final_all_categories_locale_fr_BE = all_categories_s10_fr_FR.copy()
final_all_categories_locale_fr_BE['Locale'] = 'fr_BE'

final_all_categories_locale_combined = pd.concat([all_categories_s10_nl_NL, all_categories_s10_fr_FR, final_all_categories_locale_nl_BE, final_all_categories_locale_fr_BE], ignore_index=True)

final_all_categories_locale_combined = final_all_categories_locale_combined[['ID', 'Action', 'Category Name', 'Parent Category Path', 'Hierarchy Name', 'Locale', 'Category Long Name']]


###################
## Write output
###################
print('### Output ###')

# Write the concatenated DataFrame to an Excel file
output_file_path = os.path.join(os.getcwd(), 'GS1_vs_Datamodel_Comparison5.xlsx')

with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    final_all_categories_s9.to_excel(writer, sheet_name='S9 - Category', index=False)
    final_all_categories_locale_combined.to_excel(writer, sheet_name='S10 - Category - Locale', index=False)
            
# Load the updated Excel file into a DataFrame
# comparison_df_bricks = pd.read_excel(output_file_path, sheet_name='S9 - Category')

# Display the DataFrame contents
# print(comparison_df_bricks)
   
