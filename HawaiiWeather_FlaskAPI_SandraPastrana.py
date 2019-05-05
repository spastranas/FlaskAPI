
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Adding this - DL
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# Adding this - DL
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
#______________________________________________________________________________________________________________
#                                Create data frames from the database                                  
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(engine)
#Load measurment into a DF
Mstmt = session.query(Measurement).statement 
MeasurementDF = pd.read_sql_query(Mstmt, session.bind) 

#load Station into a DF
Sstmt = session.query(Station).statement 
StationDF = pd.read_sql_query(Sstmt, session.bind) 

#Look at the latest date form measurement table
LatestDate=pd.Timestamp(MeasurementDF["date"].max())

#calculate 12 months prior to the lates date
Query12Months=LatestDate - pd.DateOffset(months=12)
Query12Months=Query12Months.date()
#load the latest 12 months of data into a DF
TwelveMonthsOfData = session.query(Measurement).filter(func.strftime( Measurement.date) >=Query12Months).statement
TwelveMonthsOfDataDF = pd.read_sql_query(TwelveMonthsOfData, session.bind)
#set index to date and sort by date

#_______________________________________________________________________________________________

#                              Preparation for precipitation page:                                         


TwelveMonthsOfData=TwelveMonthsOfDataDF[["date","prcp"]].set_index("date").sort_values("date")

#Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
TwelveMonthsOfData.reset_index(inplace=True)
PrecipitationDic={"Key":"" ,"Value":""}
PrecipitationList=[]
for index, row in TwelveMonthsOfData.iterrows():

    # iterate thru each row of the file
    PrecipitationDic["Key"] = row["date"]
    PrecipitationDic["Value"] = row['prcp']
    PrecipitationList.append(PrecipitationDic)
    PrecipitationDic={"Key":"" ,"Value":""}
#_______________________________________________________________________________________________
#                                 Preparation for stations page:                                      

StationCountDf=pd.merge(StationDF, MeasurementDF, on="station")

GroupByDF = StationCountDf.groupby(['station'])
StationCount=GroupByDF.count()
StationCount.reset_index(inplace=True)

StationActivity=StationCount[["station","id_x"]].sort_values("id_x", ascending=False)

StationDic={"Station":"" ,"Observations":""}
SationList=[]
for index, row in StationActivity.iterrows():

    # iterate thru each row of the file
    StationDic["Station"] = row["station"]
    StationDic["Observations"] = row['id_x']
    SationList.append(StationDic)
    StationDic={"Station":"" ,"Observations":""}
#_______________________________________________________________________________________________
#                                Preparation for tobs page:                                        


TempList=[]
for index, row in TwelveMonthsOfDataDF.iterrows():

    # iterate thru each row of the file
   
    Temp= row['tobs']
    TempList.append(Temp)
#____________________________________________________________________________________________________ 
#                                Preparation for dates page:                                      

def StartEndRangeTemps(start_date, end_date):
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

def StartRangeTemps(start_date):
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).all()


#______________________________________________________________________________________________________
#                                       FLASK code

# 2. Create an app
app = Flask(__name__)



# 3. Define static  flask routes
@app.route("/")
def home():
    return (f"Welcome The the Hawaii Weather API<br/> <br/>"
        
            f"Available Routes:<br/><br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/<start><br/>"
            f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")

def precipitation():
    return jsonify(PrecipitationList)


@app.route("/api/v1.0/stations")

def stations():
    return jsonify(SationList)

@app.route("/api/v1.0/tobs")

def tobs():
    return jsonify(TempList)


@app.route("/api/v1.0/<start>")

def Date(start):
    RangeList=StartRangeTemps(start)
    RangeDic={"Min":RangeList[0][0], "Mean":RangeList[0][1], "Max":RangeList[0][2]}

    return jsonify(RangeDic)



@app.route("/api/v1.0/<start>/<end>")

def Dates(start,end):
    RangeList=StartEndRangeTemps(start, end)
    RangeDic={"Min":RangeList[0][0], "Mean":RangeList[0][1], "Max":RangeList[0][2]}

    return jsonify(RangeDic)


    # Adding this - DL
@app.teardown_appcontext
def cleanup(resp_or_exc):
    print('Teardown received')
    db_session.remove()

if __name__ == '__main__':
    app.run(debug=True)