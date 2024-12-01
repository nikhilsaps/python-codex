import pandas as pd
import numpy as np

# Initialize empty DataFrames globally
almond_dataframe = pd.DataFrame()
almond_assigned_data = pd.DataFrame()
almond_unassigned_data = pd.DataFrame()
original_almond = pd.DataFrame()
dep_ldv_dataframe = pd.DataFrame()
l4inv_tt_dataframe = pd.DataFrame()
l3inv_tt1_dataframe = pd.DataFrame()
l3inv_tt2_dataframe = pd.DataFrame()
l3inv_mp1_dataframe = pd.DataFrame()
l3inv_mp2_dataframe = pd.DataFrame()
l4_login = pd.DataFrame()
l3_login = pd.DataFrame()

# Variables for filtering
dep_ldv_tt = ["55764741", "75681999", "119332182", "32009623", "109033057", "76829386", "94763433", "94351206", "94639053", "106230465"]
l4inv_tt = ["d113007432", "139889713", "141997345"]
l3inv_tt1 = ["139889713", "133046258", "25110413", "134859146"]
l3inv_tt2 = ["31672832", "51010445", "98426315", "66849682", "96696746"]
l3inv_mp1 = ["4", "338801", "623225021", "1", "3"]
l3inv_mp2 = ["111172", "338811", "6"]

def task_splitter_prep():
    global almond_dataframe, dep_ldv_dataframe, l4inv_tt_dataframe, l3inv_tt1_dataframe, l3inv_tt2_dataframe, l3inv_mp1_dataframe, l3inv_mp2_dataframe, original_almond, l4_login, l3_login
    almond_dataframe = pd.read_csv("almond.csv")
    l4_login = pd.read_csv("l4_login.csv")
    l3_login = pd.read_csv("l3_login.csv")
    almond_dataframe.drop_duplicates(subset=['CUSTOMER_ID','ANNOS'],keep='first',inplace=True)
    original_almond = almond_dataframe
    almond_dataframe.insert(0, "login", np.nan)
    almond_dataframe.insert(2, "type", np.nan)
    original_almond = almond_dataframe

    dep_ldv_dataframe = almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(dep_ldv_tt), case=False, na=False)]
    almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(dep_ldv_dataframe.index)]

    l4inv_tt_dataframe = almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(l4inv_tt), case=False, na=False)]
    almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(l4inv_tt_dataframe.index)]

    l3inv_tt1_dataframe = almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(l3inv_tt1), case=False, na=False)]
    almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(l3inv_tt1_dataframe.index)]

    l3inv_tt2_dataframe = almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(l3inv_tt2), case=False, na=False)]
    almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(l3inv_tt2_dataframe.index)]

    l3inv_mp1_dataframe = almond_dataframe[almond_dataframe['MARKETPLACE_ID'].astype(str).isin(l3inv_mp1)]
    almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(l3inv_mp1_dataframe.index)]

    l3inv_mp2_dataframe = almond_dataframe[almond_dataframe['MARKETPLACE_ID'].astype(str).isin(l3inv_mp2)]
    almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(l3inv_mp2_dataframe.index)]


def assign_l4_task(task_df, login_df, output_login_csv='l4_logins_output.csv'):
    if 'login' not in task_df.columns or 'count' not in login_df.columns:
        raise ValueError("Missing required columns in task or login DataFrame")

    task_df = task_df.copy()  # Ensure it's a copy of the DataFrame to avoid issues with slices
    task_df['login'] = task_df['login'].astype('object')

    login_df_sorted = login_df.sort_values(by='count', ascending=False).reset_index(drop=True)

    # Create a dictionary to keep track of how many tasks each login has been assigned
    login_task_count = {login: 0 for login in login_df_sorted['login']}

    # Total available task capacity across all logins
    total_capacity = login_df_sorted['count'].sum()

    # Total number of tasks in task_df
    total_tasks = len(task_df)

    # Check if there are more tasks than the total available capacity
    if total_tasks > total_capacity:
        print(f"Warning: The number of tasks ({total_tasks}) exceeds the total capacity of L4 ({total_capacity}). Some tasks will remain unassigned.")

    # Loop through each row in the task dataframe (each row represents a case)
    for idx, task in task_df.iterrows():
        assigned = False
        
        # Iterate over sorted login DataFrame to assign tasks to logins
        for _, login_row in login_df_sorted.iterrows():
            login = login_row['login']
            max_count = login_row['count']
            
            # Check if the login has already been assigned the maximum number of tasks
            if login_task_count[login] < max_count:
                task_df.loc[idx, 'login'] = login  # Assign the task to this login
                login_task_count[login] += 1  # Increment the task count for this login

                # Update the count in the login DataFrame to reflect the remaining tasks
                login_df.loc[login_df['login'] == login, 'count'] -= 1

                assigned = True
                break  # Exit the loop once the task is assigned

        # If no login could be assigned, leave the task unassigned (login column stays empty or None)
        if not assigned:
            task_df.loc[idx, 'login'] = None  # Use None or np.nan for unassigned tasks

    # Save the updated login DataFrame to a CSV
    login_df.to_csv(output_login_csv, index=False)

    # Create separate DataFrames for assigned and unassigned tasks
    assigned_task_df = task_df[task_df['login'].notna()]  # Not NaN or None
    unassigned_task_df = task_df[task_df['login'].isna()]  # Is NaN or None (unassigned)

    # Optional: Also check for blank values (''), in case any blank values exist
    unassigned_task_df = unassigned_task_df[(unassigned_task_df['login'].isna()) | (unassigned_task_df['login'] == '')]


    # Return the assigned and unassigned task DataFrames
    return assigned_task_df, unassigned_task_df




def assign_tasks_to_agents(task_df, login_df):
    # Create a copy of the login dataframe to track updates
    updated_login_df = login_df.copy()

    # Create an empty DataFrame to hold assigned tasks
    assigned_tasks = pd.DataFrame()

    # Create an empty DataFrame to hold unassigned tasks
    unassigned_tasks = pd.DataFrame()

    # Create a list of agents and their respective max task counts
    agents = updated_login_df['login'].tolist()
    task_limits = updated_login_df['count'].tolist()

    # Flatten out all available tasks by agent, respecting their limits
    agent_task_distribution = []
    for agent, limit in zip(agents, task_limits):
        agent_task_distribution.extend([agent] * limit)
    
    # If there are more tasks than agents can handle, we have unassigned tasks
    if len(agent_task_distribution) < len(task_df):
        unassigned_tasks = task_df.iloc[len(agent_task_distribution):].copy()  # Make sure it's a copy
        task_df = task_df.iloc[:len(agent_task_distribution)].copy()  # Make sure it's a copy

    # Assign the tasks to agents in a round-robin fashion
    task_df['login'] = np.random.choice(agent_task_distribution, len(task_df), replace=False)

    # Now, update the task counts in login dataframe
    task_count = task_df['login'].value_counts()

    for agent, count in task_count.items():
        updated_login_df.loc[updated_login_df['login'] == agent, 'count'] -= count

    # Save the updated login dataframe to a CSV (or you could return it)
    updated_login_df.to_csv('updated_logins.csv', index=False)

    return task_df, unassigned_tasks



def assign_logins_to_tasks_40_percent(task_df, login_df, output_csv="updated_logins.csv"):
    # Ensure both dataframes have the necessary columns
    if 'login' not in task_df.columns or 'count' not in login_df.columns:
        raise ValueError("Missing required columns in task or login DataFrame")

    task_df = task_df.copy()  # Ensure it's a copy of the DataFrame to avoid issues with slices
    task_df['login'] = task_df['login'].astype('object')

    # Sort the login DataFrame by 'count' to prioritize logins with higher capacity
    login_df_sorted = login_df.sort_values(by='count', ascending=False).reset_index(drop=True)

    # Create a dictionary to keep track of how many tasks each login has been assigned
    login_task_count = {login: 0 for login in login_df_sorted['login']}

    # Loop through each row in the task dataframe (each row represents a task)
    for idx, task in task_df.iterrows():
        assigned = False
        
        # Iterate over sorted login DataFrame to assign tasks to logins
        for _, login_row in login_df_sorted.iterrows():
            login = login_row['login']
            original_count = login_row['count']

            # Calculate the max number of tasks that can be assigned to this login (40% of their original capacity)
            max_assignable_tasks = int(original_count * 0.4)
            
            # Check if the login has been assigned less than 40% of its tasks
            if login_task_count[login] < max_assignable_tasks:
                task_df.loc[idx, 'login'] = login  # Use .loc to set the value for the task
                login_task_count[login] += 1  # Increment the task count for this login
                
                # Optionally, update the count in the login DataFrame to reflect remaining capacity
                login_df.loc[login_df['login'] == login, 'count'] -= 1

                assigned = True
                break  # Exit the loop once the task is assigned

        # If no login could be assigned, leave the task unassigned (login column stays empty)
        if not assigned:
            task_df.loc[idx, 'login'] = None  # Optionally leave this empty or apply another strategy

    # Split the DataFrame into assigned and unassigned tasks
    assigned_tasks = task_df[task_df['login'].notna()]
    unassigned_tasks = task_df[task_df['login'].isna()]

    # Save the updated login_df to a CSV file
    login_df.to_csv(output_csv, index=False)

    return assigned_tasks, unassigned_tasks


def merge_dataframe(df1, df2):
    merged_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    return merged_df


def merge_login(df1, df2):
    merged_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    
    # Remove rows where 'count' is 0
    merged_df = merged_df[merged_df['count'] != 0]   
    return merged_df


def main():
    global almond_dataframe, dep_ldv_dataframe, l4inv_tt_dataframe, l3inv_tt1_dataframe, l3inv_tt2_dataframe, l3inv_mp1_dataframe, l3inv_mp2_dataframe, original_almond, l4_login, l3_login, almond_assigned_data
    task_splitter_prep()    
    print("Total Almond Cases:", len(original_almond))
    print("\nDep LDV DataFrame:", len(dep_ldv_dataframe))
    print("\nL4 Inv TT :", len(l4inv_tt_dataframe))
    print("\nL3 Inv TT1 :", len(l3inv_tt1_dataframe))
    print("\nL3 Inv TT2 :", len(l3inv_tt2_dataframe))
    print("\nL3 Inv MP1 :", len(l3inv_mp1_dataframe))
    print("\nL3 Inv MP2 :", len(l3inv_mp2_dataframe))
    print("\nL3 Inv MPa :", len(almond_dataframe))
    print(almond_dataframe.head())
    
    # L4 assigner  
    assigned_l4_task, unassigned_l4_task = assign_l4_task(l4inv_tt_dataframe, l4_login)
    print("Assigned Tasks:", len(assigned_l4_task)) 
    print("Unassigned Tasks:", len(unassigned_l4_task))
    # assigned_l4_task.to_csv("assigned_l4_task.csv")
    # unassigned_l4_task.to_csv("unassigned_l4_task.csv")
    rest_l4_login= pd.read_csv("l4_logins_output.csv")
    l3_login=merge_login(l3_login,rest_l4_login)

    almond_assigned_data= assigned_l4_task

    assigned_l4l3_task, unassigned_l4l3_task = assign_tasks_to_agents(unassigned_l4_task, l3_login)

    print("Assigned Tasks:", len(assigned_l4l3_task)) 
    print("Unassigned Tasks:", len(unassigned_l4l3_task))
    #saving a copy of assigned work 
    almond_assigned_data =merge_dataframe(almond_assigned_data,assigned_l4l3_task)
    

    l3inv_tt1_dataframe= merge_dataframe(l3inv_tt1_dataframe,unassigned_l4l3_task)

    updated_l3_login= pd.read_csv("updated_logins.csv")

    assigned_l3_task, unassigned_l3_task = assign_logins_to_tasks_40_percent(l3inv_tt1_dataframe, updated_l3_login)
    print("Assigned Tasks:", len(assigned_l3_task)) 
    print("Unassigned Tasks:", len(unassigned_l3_task))

    almond_unassigned_data= unassigned_l3_task
    almond_assigned_data =merge_dataframe(almond_assigned_data,assigned_l3_task)

    updated_l3_login= pd.read_csv("updated_logins.csv")

    assigned_l32_task, unassigned_l32_task = assign_logins_to_tasks_40_percent(l3inv_tt2_dataframe, updated_l3_login)
    print("Assigned Tasks:", len(assigned_l32_task)) 
    print("Unassigned Tasks:", len(unassigned_l32_task))
    almond_unassigned_data= merge_dataframe(almond_unassigned_data,unassigned_l32_task)
    almond_assigned_data =merge_dataframe(almond_assigned_data,assigned_l32_task)


    updated_l3_login= pd.read_csv("updated_logins.csv")
    assigned_l3m1_task, unassigned_l3m1_task = assign_tasks_to_agents(l3inv_mp1_dataframe, updated_l3_login)
    print("Assigned Tasks:", len(assigned_l3m1_task)) 
    print("Unassigned Tasks:", len(unassigned_l3m1_task))
    almond_unassigned_data= merge_dataframe(almond_unassigned_data,unassigned_l3m1_task)
    almond_assigned_data =merge_dataframe(almond_assigned_data,assigned_l3m1_task)


    updated_l3_login= pd.read_csv("updated_logins.csv")
    assigned_l3m2_task, unassigned_l3m2_task = assign_tasks_to_agents(l3inv_mp2_dataframe, updated_l3_login)
    print("Assigned Tasks:", len(assigned_l3m2_task)) 
    print("Unassigned Tasks:", len(unassigned_l3m2_task))
    almond_unassigned_data= merge_dataframe(almond_unassigned_data,unassigned_l3m2_task)
    almond_assigned_data =merge_dataframe(almond_assigned_data,assigned_l3m2_task)

    updated_l3_login= pd.read_csv("updated_logins.csv")
    assigned_l3ma_task, unassigned_l3ma_task = assign_tasks_to_agents(almond_dataframe, updated_l3_login)
    print("Assigned Tasks:", len(assigned_l3ma_task)) 
    print("Unassigned Tasks:", len(unassigned_l3ma_task))
    almond_unassigned_data= merge_dataframe(almond_unassigned_data,unassigned_l3ma_task)
    almond_assigned_data =merge_dataframe(almond_assigned_data,assigned_l3ma_task)

    print("assigned almond total",len(almond_assigned_data))
    print("unassigned almond total",len(almond_unassigned_data))
    print("dep_ldv",len(dep_ldv_dataframe))









   

if __name__ == "__main__":
    main()
