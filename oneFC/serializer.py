import pandas as pd
import json
import sys

#this method helps to serialize a pandas dataframe
def serialize(df, op_file):
    df_data = df.to_dict(orient="records")
    data = {}
    data["Floor Access Event"]=df_data
    print(type(data))
    data=json.dumps(data, default=str)
    with open(op_file, "w") as file:
        file.write(data)

#main method
if __name__=="__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    df = pd.read_csv(input_path)
    print(df.dtypes)
    #converting the "Person Id" column from inst to string
    df["Person Id"]=df["Person Id"].apply(str)
    print(df.dtypes)
    #converting the "Floor Access DAtetime" column to datetime type
    df["Floor Access DateTime"]=pd.to_datetime(df["Floor Access DateTime"])
    print(df.dtypes)
    serialize(df,output_path)


#to run this code """python serializer.py <input_path> <output_path>"""