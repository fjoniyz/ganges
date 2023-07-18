import prognose
import pandas
import datetime
import calendar
import time

def create_TaskSimEvCharging(x, power) :
    #each max is just min value plus one hour

    # Define the input date and time string
    # input_start_time_loading = x.start_time_loading
    # dt = datetime.fromisoformat(input_start_time_loading)
    # minutes_start_time_loading = dt.hour * 60 + dt.minute
    #
    # input_end_time_loading = x.end_time_loading
    # dt = datetime.fromisoformat(input_end_time_loading)
    # minutes_end_time_loading = dt.hour * 60 + dt.minute
    min_start = int(min(x["start_time_loading"]) / 16581777)
    max_start = int(max(x["start_time_loading"]) / 16581777)
    print("Min Start", min_start)
    print("Max Start", max_start)
    min_duration = int(min(x["duration"]))
    max_duration = int(max(x["duration"]))
    print(min_duration)
    print(max_duration)

    min_demand = int(min(x["kwh"]))
    max_demand = int(max(x["kwh"]))
    print(min_demand)
    print(max_demand)
    return prognose.TaskSimEvCharging(min_duration, max_duration, min_demand, max_demand, min_start, max_start, power)

def read_csv(file: str):
    print(f"Reading %s ..." % file)
    dataset = pandas.read_csv(file)
    # TODO: crate array with right datasets
    good_dataset = {}
   
    good_dataset["start_time_loading"] = [calendar.timegm(time.strptime(s, '%Y-%m-%d %H:%M:%S')) for s in dataset.start_time_loading]
    good_dataset["end_time_loading"] = [calendar.timegm(time.strptime(s, '%Y-%m-%d %H:%M:%S')) for s in dataset.end_time_loading]

    #good_dataset["duration"] = good_dataset["end_time_loading"] - good_dataset["start_time_loading"]
    good_dataset["duration"] = [x - y for x, y in zip(good_dataset["end_time_loading"], good_dataset["start_time_loading"])]
   
    good_dataset["kwh"] = [int(s) for s in dataset.kwh]

    return good_dataset

# TODO: read csv message
#x = json.loads(msg.value(), object_hook=lambda d: SimpleNamespace(**d))
x = read_csv("./data/electromobility_data.csv")

prognose.random.seed(prognose.pd.Timestamp.utcnow().dayofyear)
power = [1, 2, 3, 4]
task_instance = create_TaskSimEvCharging(x, power)
d = {"col1": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
             task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col2": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col3": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col4": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col5": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col7": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col8": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col9": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col10": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
    "col11": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
            task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
             }

df = prognose.DataFrame(data=d)
print(prognose.simulate_ev_forecast(df=df, cfg=task_instance))