import pandas as pd
import pprint
class RecordReader:
    db=None

    def __init__(self,db):
        self.db =   db

    def reset_mongodb(self,path_name):
        self.db["records"].drop()
        df = pd.read_csv(path_name)
        df = self._clean_df(df)
        records=df.to_dict(orient="records")
        #print(records)
        self.db["records"].insert_one(self._construct_document("melbourne_housing",records))
        #print(self.db["records"])


    def _clean_df(self,df):
        df = df[df.Price.notnull()]
        df.Car.fillna(0, inplace=True)
        df.Landsize.fillna(0,inplace=True)
        df.dropna(inplace=True)
        return df

    def _construct_document(self,document_name,records):
        document=dict()
        document["title"]=document_name
        document["entry"]=records
        return document

    def count_record_in_db(self):
        return  self.db["records"].count()

    def get_records_by_title(self,title):
        return self.db["records"].find_one({"title":title})

    def print_records_by_title(self,title):
        pprint.pprint(self.db["records"].find_one({"title":title}))

    def to_dataframe(self,title):
        records=self.get_records_by_title(title)
        if (records is not None):
            records=records["entry"]
        # print(pd.DataFrame(records))
        print("Dataframe loads correctly;")
        return pd.DataFrame(records)

    def groupBy(self,title,col):
        df = self.to_dataframe(title)
        df = df[[col,"Address"]]
        df=df.groupby(by=col).count().rename(columns={"Address":"counter"}).reset_index()
        result=[["Surburb","number of transaction"]]
        for index, row in df.iterrows():
            result.append([row[0],row[1]])
        return result