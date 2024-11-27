import pandas as  pd
import numpy as np
# Original data of the Almond

almond_dataframe= pd.read_csv("almond.csv")
len(almond_dataframe)

# removed deuplicated data
almond_dataframe.drop_duplicates(subset=['CUSTOMER_ID','ANNOS'],keep='first',inplace=True)
print(len(almond_dataframe))

#  sim of almond for priority and assignemnt 
dep_ldv_tt= ["55764741","75681999","119332182","32009623","109033057","76829386","94763433","94351206","94639053","106230465"]
fi_l4inv_tt=["d113007432","139889713","141997345"]
se_l3inv_tt=["139889713","133046258","25110413","134859146"]
th_l3inv_tt=["31672832","51010445","98426315","66849682","96696746"]
fo_l3inv_mp=["4","338801","623225021","1","3"]
fi_l3inv_mp=["111172","338811","6"]

# adding the column for login and type 
almond_dataframe.insert(0,"logins",np.nan)
almond_dataframe.insert(2,"type",np.nan)
print("Total Almond Case :",len(almond_dataframe))

# deprioritized the ldv case
dep_ldv_dataframe = almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(dep_ldv_tt), case=False, na=False)]
almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(dep_ldv_dataframe.index)]
print("Deprioritized ldv case :",len(dep_ldv_dataframe))


# dataframe for the L4 investigator  pried tts
fi_l4inv_dataframe =almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(fi_l4inv_tt), case=False, na=False)]
almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(fi_l4inv_dataframe.index)]
print("L4  inv tt case :",len(fi_l4inv_dataframe))
fi_l4inv_dataframe.to_csv("task.csv",index=False)



# dataframe for 2nd best priority l3 investigator 
se_l3inv_dataframe =almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(se_l3inv_tt), case=False, na=False)]
almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(se_l3inv_dataframe.index)]
print("L3  inv tt case :",len(se_l3inv_dataframe))


# dataframe for 2nd best priority l3 investigator 
th_l3inv_dataframe =almond_dataframe[almond_dataframe['ANNOS'].str.contains('|'.join(th_l3inv_tt), case=False, na=False)]
almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(th_l3inv_dataframe.index)]
print("L3  inv 2nd tt case :",len(th_l3inv_dataframe))


# dataframe for mp level  pried 
fo_l3inv_mp_dataframe = almond_dataframe[almond_dataframe['MARKETPLACE_ID'].astype(str).isin(fo_l3inv_mp)]
almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(fo_l3inv_mp_dataframe.index)]
print("L3 mp tt case :",len(fo_l3inv_mp_dataframe))


# dataframe for last mp pried tts 
fi_l3inv_mp_dataframe = almond_dataframe[almond_dataframe['MARKETPLACE_ID'].astype(str).isin(fi_l3inv_mp)]
almond_dataframe = almond_dataframe[~almond_dataframe.index.isin(fi_l3inv_mp_dataframe.index)]
print("L3 mp 2nd tt case :",len(fi_l3inv_mp_dataframe))

print("leftover Almond Case :",len(almond_dataframe))