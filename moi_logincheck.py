import pandas as pd

data_str = "abc|70,aba|45,abd|68,abb|45,fdg|34"


data_pairs = data_str.split(',')

data_tuples = [tuple(pair.split('|')) for pair in data_pairs]

login_df = pd.DataFrame(data_tuples, columns=['login', 'count'])

login_df['count'] = pd.to_numeric(login_df['count'])


print(login_df)
login_df.to_csv("login.csv",index=False)