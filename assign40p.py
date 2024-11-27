import pandas as pd

def assign_logins_to_tasks_40_percent(task_df, login_df):
    # Ensure that both dataframes have the necessary columns
    if 'login' not in task_df.columns or 'count' not in login_df.columns:
        raise ValueError("Missing required columns in task or login DataFrame")

    # Convert the 'login' column in the task dataframe to object type (to handle strings)
    task_df['login'] = task_df['login'].astype('object')

    # Sort the login DataFrame by 'count' to prioritize logins with higher capacity
    login_df_sorted = login_df.sort_values(by='count', ascending=False).reset_index(drop=True)

    # Create a dictionary to keep track of how many tasks each login has been assigned
    login_task_count = {login: 0 for login in login_df_sorted['login']}

    # Loop through each row in the task dataframe (each row represents a case)
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
                task_df.at[idx, 'login'] = login  # Assign the login to the task
                login_task_count[login] += 1  # Increment the task count for this login
                
                # Optionally, update the count in the login DataFrame to reflect remaining capacity
                login_df.loc[login_df['login'] == login, 'count'] -= 1

                assigned = True
                break  # Exit the loop once the task is assigned

        # If no login could be assigned, leave the task unassigned (login column stays empty)
        if not assigned:
            task_df.at[idx, 'login'] = None  # Optionally leave this empty or apply another strategy

    return task_df, login_df

# Load task and login data from CSV files
task = pd.read_csv("task.csv")
login = pd.read_csv("login.csv")

# Assign logins to tasks using 40% of their count and update the login count
assigned_task_df, updated_login_df = assign_logins_to_tasks_40_percent(task, login)

# Save the updated task and login DataFrames to new CSV files
assigned_task_df.to_csv("assigned_40_percent.csv", index=False)
updated_login_df.to_csv("updated_login_40_percent.csv", index=False)
