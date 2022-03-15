import dask.array as da
import dask.dataframe as dd
import pandas as pd

def generate_single_GTtrajectory(df_car):
    print('test')
    df_list=dd.DataFrame()

    df_list["x_position"]=df_car["x_position"]
    df_list["y_position"]=df_car["y_position"]


    listdict=df_list.to_dict("list") #turn into arrays
    return listdict
    
df = pd.DataFrame({'car_ID': [1, 1, 1, 2, 2],
                   'x_position': [100, 102, 104, 40, 42],
                   'y_position': [3, 3, 3, 2, 2]})
data_to_append = []
for car_ID in range(3, 5):
    for i in range(10):
        data_to_append.append({'car_ID': car_ID,
                   'x_position': car_ID * 100 + i,
                   'y_position': car_ID})
df2 = pd.DataFrame.from_records(data_to_append)
df = pd.concat([df, df2], ignore_index=True)
print(df)
ddf = dd.from_pandas(df, npartitions=4)

ddf2 = ddf.groupby(['car_ID'])
print(ddf2.mean().compute())
result=ddf.groupby(["car_ID"]).apply(generate_single_GTtrajectory, meta={'x_position': 'f8',
                                                                          'y_position': 'f8'}).compute()
print(result.compute())
#print (ddf.compute())
