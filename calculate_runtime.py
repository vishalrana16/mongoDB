import pymongo
from bson import ObjectId
import datetime
import matplotlib.pyplot as plt
import numpy as np

connection = pymongo.MongoClient('localhost',27017)

db = connection['production_count'] #database has been named as production_count
production = db['production']
print('Database is connected!!!')

hourly_runtime = db['hourly_runtime']

def insert_data(data):
	document = hourly_runtime.insert_one(data)
	return document.inserted_id

def get_two_document(i):
	pipeline = [{"$project":{
				    	  "day":{"$dayOfMonth":{"date":"$published_at"}},	
				    	  "hour":{"$hour":{"date":"$published_at"}},
						  "minute":{"$minute":{"date":"$published_at"}},
						  "second":{"$second":{"date":"$published_at"}},
						  "published_at":1,
						  "value":1
				}},
				{"$skip":i},
				{"$limit":2} 
			    ]			    
	return list(db.production.aggregate(pipeline))

def get_length_docs():
	pipeline = [ {"$project":{
    	  			"_id":1
    	  			}
    	  		 }
			   ]
	return len(list(db.production.aggregate(pipeline)))

#print(get_length_docs())

def check_diff_in_values(doc0, doc1):
	if (doc1["value"]-doc0["value"])>0:
		return True
	return False

def get_time_difference(doc0, doc1):
	doc1_time = doc0["published_at"]
	doc2_time = doc1["published_at"]
	time_diff = doc2_time-doc1_time
	return time_diff.total_seconds()

def convert_sec_to_time(val):
	val = float("{0:.2f}".format(val/60))
	val= '{0:02.0f}:{1:02.0f}'.format(*divmod(val * 60, 60))
	return val

total_length = get_length_docs()

runtime_per_hour = []
starttime_per_hour = []
for i in range(24):
	runtime_per_hour.append(0)
	starttime_per_hour.append(0)

for i in range(total_length-1):
	doc = get_two_document(i)
	#print(doc)
	get_doc = doc #get the two document form the function "get_two_document" it will be a list [{ document 1 }, { document 2 }]
	if get_doc[0]["day"] == get_doc[1]["day"]:
		if get_doc[0]["hour"] ==  get_doc[1]["hour"]:
			if starttime_per_hour[get_doc[0]["hour"]] == 0:
				starttime_per_hour[get_doc[0]["hour"]] = get_doc[0]["published_at"]
			if(check_diff_in_values(get_doc[0], get_doc[1])):
				runtime_per_hour[get_doc[0]["hour"]] += get_time_difference(get_doc[0], get_doc[1]) #it will give time differene between two date time
		else:
			if(check_diff_in_values(get_doc[0], get_doc[1])):
				start_date = get_doc[1]["published_at"]
				starttime_per_hour[get_doc[1]["hour"]] = datetime.datetime(start_date.year, start_date.month,start_date.day, start_date.hour, start_date.minute,0)
				runtime_per_hour[get_doc[0]["hour"]] += 59-get_doc[0]["second"]
				runtime_per_hour[get_doc[1]["hour"]] += get_doc[1]["second"]
	else:
		if(check_diff_in_values(get_doc[0], get_doc[1])):
				runtime_per_hour[get_doc[0]["hour"]] += 59-get_doc[0]["second"]



for i in range(len(runtime_per_hour)):
	runtime_per_hour[i] = convert_sec_to_time(runtime_per_hour[i])	

for i in range(len(runtime_per_hour)):
	dic_data = {}
	dic_data["start_time"] = starttime_per_hour[i]
	dic_data["runtime"] = runtime_per_hour[i]
	insert_data(dic_data)

print("Successfully created collection!!!")

"""Some extra work"""
runtime_hr_float = []
for i in runtime_per_hour:
	x,y = i.split(":")
	runtime_hr_float.append(int(x)+ (int(y)*.01))

label  = []
for i in range(len(runtime_hr_float)):
	label.append("hour_"+str(i+1))

#creating graph
def plot_x():
    # plotting runtime graph for each hour    
    index = np.arange(len(label))
    plt.bar(index, runtime_hr_float)
    plt.xlabel('hours', fontsize=10)
    plt.ylabel('Runtime in min', fontsize=10)
    plt.xticks(index, label, fontsize=10, rotation=30)
    plt.title('Runtime time graph')
    
    plt.show()
    
plot_x()




