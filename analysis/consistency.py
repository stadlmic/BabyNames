import pandas as pd

national = pd.read_csv('data/landing/unzip/NationalNames.csv')
state = pd.read_csv('data/landing/unzip/StateNames.csv')

#datasets dont start at the same time... this will be respected in the final report
national['Year'].min()  #-> 1880
state['Year'].min() # -> 1910
national['Year'].max()
state['Year'].max()

#quick and dirty basic stuff
national["Gender"].nunique()
state["Gender"].nunique()
national["Gender"].unique()
state["Gender"].unique()
national.isnull().sum()
state.isnull().sum()
national.groupby("Year")["Count"].sum()
state.groupby("Year")["Count"].sum()




#lets check consistency between national and state names

#after when agregated as sum by ['Name','Year','Gender'], data should by somewhat equal
state_agregate = state.groupby(['Name','Year','Gender'])['Count'].sum().reset_index()

#just based on high level check, number of records in national dataset is about 10% higher.
# But this can be due to rows with less than 5 record are dropped due to anonymization ->let's dig a little deeper to be sure about this
# Also national data start earlier
state['Count'].sum()
national['Count'].sum()

#proportion of total sum is a metric independent of total size
state_agregate['ProportionCount'] = state_agregate['Count'] / sum(state_agregate['Count'])
national['ProportionCount'] = national['Count'] / sum(national['Count'])
merge = state_agregate.merge(national, on=['Name', 'Gender', 'Year'], how = 'outer', suffixes=('_state', '_national'))
merge['ProportionCountRelDiff'] = (merge['ProportionCount_national'] - merge['ProportionCount_state'] ) /merge['ProportionCount_state']
merge['ProportionCountRelDiffAbs'] = merge['ProportionCountRelDiff'].abs()
merge['ProportionCountRelDiffPercetage'] = merge['ProportionCountRelDiff'] * 100


import matplotlib.pyplot as plt
#let's compare the promotion count agains each other -> they look well aligned
merge.plot(x='ProportionCount_national', y='ProportionCount_state')
plt.show()

#However, when comparing prortion difference with regards to size (count in this example),
#we see that the proportions are well aligned only for the more popular names
merge[merge['Count_national'] > (5*51)](x='ProportionCountRelDiffPercetage', y='Count_national')
plt.show()


#so when we compare the promotion counts agains each other again,
#this time filtering only the names given less that 100 times per year
#we see that the proportions don't match at all
merge[merge['Count_national'] < 250].plot(x='ProportionCount_national', y='ProportionCount_state')
plt.show()

#also we can double check by looking on the original datasets
check_key = merge.sort_values(by = 'ProportionCountRelDiffAbs', ascending=False).head(10)[['Name', 'Gender', 'Year']]
pd.merge(national, check_key, how='inner')
pd.merge(state, check_key, how='inner')




check_key = merge.sort_values(by = 'ProportionCountRelDiffAbs', ascending=False).head(10)
check_key2 = merge[merge['Count_national'] > (5*51)].sort_values(by = 'ProportionCountRelDiffAbs', ascending=False).head(10)




