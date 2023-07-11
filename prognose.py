import pandas as pd
import numpy as np
import random
class TaskSimEvCharging:
    def __init__(self, min_duration, max_duration, min_demand, max_demand, min_start, max_start, power):
        self.min_start = min_start
        self.max_start = max_start
        self.min_duration = min_duration
        self.max_duration = max_duration
        self.min_demand = min_demand
        self.max_demand = max_demand
        self.power = power

class EvChargingPlan:
    def __init__(self, charge, start, mstart, duration, mduration, demand, mdemand, power):
        self.charge = charge
        self.start = start
        self.mstart = start + mstart * 30
        self.duration = duration
        self.mduration = duration + mduration * 60
        self.demand = demand 
        self.mdemand = demand + mdemand * 5
        self.power = power


def _simulate_ev_forecast(df: pd.DataFrame, cfg: TaskSimEvCharging) -> pd.DataFrame:

    """

    The function simulates an EV forecast as well as the measured values.

 

    The complete process does not require any intermediate storage. New events are

    rolled on a daily basis. A constant seed is used by the day number in the

    year. This keeps the simulation the same within a day.

 

    :param df:

    :param cfg:

    :return: Dataframe with results

    """

    # decission variables through specificly configured normal distributions

    random.seed(

        pd.Timestamp.utcnow().dayofyear

    )  # initialization of seed by day number of year
    dec_charge = pd.DataFrame(np.random.randint(0,2,size=(4, 4)), columns=list('ABCD'))
    # dec_charge = np.random.normal(0.5, 0.1)

    root_now = 15

    sampling_time = '20D' #d means days. So in this case we have a sampling time of 20 days

    #=====================All random dataframes=====================
    dec_start = pd.DataFrame(np.random.normal((cfg.max_start - cfg.min_start) * 0.15, cfg.min_start + (cfg.max_start - cfg.min_start) * 0.5, size=(100,4)), columns=list('ABCD'))

    #This may be the noise that gets added to each entry
    #This means that the noise is either 0,1 or 2

    noise = pd.DataFrame(np.random.normal(0,3,size=(100, 4)), columns=list('ABCD'))

    # Why are these 2 variables calculated like this??

    dec_duration = pd.DataFrame(np.random.normal((cfg.max_duration - cfg.min_duration) * 0.15, cfg.min_duration + (cfg.max_duration - cfg.min_duration) * 0.5, size=(100, 4)), columns=list('ABCD'))

    dec_demand = pd.DataFrame(np.random.normal((cfg.max_duration - cfg.min_duration) * 0.15, cfg.min_duration + (cfg.max_duration - cfg.min_duration) * 0.5, size=(100, 4)), columns=list('ABCD'))

    #================================================================

    # generate ev event for one charging points

    plans = []

    #Number of unique days in the index

    nr_days = (

        df.index.unique().size

    )  # get number of day's indirect over input time series index

    print("Nr days", nr_days)

    # make decisions for each day

    # takes charing place or not

    charge_dec = dec_charge.sample(nr_days) <= 0.8

    # how many minutes after midnight starts the charging process

    start_dec = dec_start.sample(nr_days)  # for forecast

    mstart_dec = noise.sample(nr_days)  # for measurement (-1d)

    # how long is the charging process

    duration_dec = dec_duration.sample(nr_days)  # for forecast

    mduration_dec = noise.sample(nr_days)  # fore measurement

    # how much energy is needed

    demand_dec = dec_demand.sample(nr_days)  # for forecast

    mdemand_dec = noise.sample(nr_days)  # for measurement

    # at what power level takes the charging place

    power_dec = random.choices(cfg.power, k=nr_days)

    # put decissions together in an event list

    for i in range(nr_days):
        print(start_dec.get("A").first_valid_index())
        plans.append(
            EvChargingPlan(
                charge=charge_dec.get("A").first_valid_index(),

                start=int(start_dec.get("A").first_valid_index()) if charge_dec.get("A") is not None else -1,

                mstart=int(start_dec.get("A").first_valid_index() + mstart_dec.get("A").first_valid_index() * 30)

                if charge_dec.get("A").first_valid_index() is not None

                else -1,

                duration=int(duration_dec.get("A").first_valid_index()) if charge_dec.get("A").first_valid_index() is not None else -1,

                mduration=int(duration_dec.get("A").first_valid_index() + mduration_dec.get("A").first_valid_index() * 60) if charge_dec.get("A") is not None else -1,

                demand=demand_dec.get("A").first_valid_index() if charge_dec.get("A") is not None else -1,

                mdemand=demand_dec.get("A").first_valid_index() + mdemand_dec.get("A") * 5 if charge_dec.get("A") is not None else -1,

                power=power_dec[i] if charge_dec.get("A") is not None else -1,

            )

        )

    # build result time series

    pipo = np.array([])

    mpipo = np.array([])

    demand = np.array([])

    mdemand = np.array([])

    power = np.array([])

    mpower = np.array([])

    od_pipo = np.zeros(12 * 60)

    od_mpipo = np.zeros(12 * 60)

    od_demand = np.zeros(12 * 60)

    od_mdemand = np.zeros(12 * 60)

    od_power = np.zeros(12 * 60)

    od_mpower = np.zeros(12 * 60)

    for item in plans:

        d_pipo = np.zeros(36 * 60)

        d_mpipo = np.zeros(36 * 60)

        d_demand = np.zeros(36 * 60)

        d_mdemand = np.zeros(36 * 60)

        d_power = np.zeros(36 * 60)

        d_mpower = np.zeros(36 * 60)

        if item.charge is not None:

            d_pipo[item.start - 1 : item.start - 1 + item.duration] = 1

            d_mpipo[item.mstart - 1 : item.mstart - 1 + item.mduration] = 1

            d_demand[item.start - 1 : item.start - 1 + item.duration] = (

                item.demand / item.duration

            )

            d_mdemand[item.mstart - 1 : item.mstart - 1 + item.mduration] = (

                item.demand / item.mduration

            )

            # power shuts down, after demand is full filled (in minute steps)

            duration = np.ceil(item.demand / item.power * 60)

            if item.duration < duration:

                duration = item.duration

            d_power[item.start - 1 : item.start - 1 + duration] = item.power

            # measured power

            for k in range(item.mstart - 1, 2160):

                np.random.seed(pd.Timestamp.utcnow().dayofyear)
                value = item.power - abs(noise.sample(1)) * 0.1 * item.power
                print(type(value))
                if value.get("A").first_valid_index() > d_mpower[1]:
                    d_mpower[k] = value.get("A").first_valid_index()
                else:
                    d_mpower[k] = item.power - abs(noise.sample(1)) * 0.1 * item.power

        # add history

        d_pipo[: 12 * 60] = d_pipo[: 12 * 60] + od_pipo

        d_mpipo[: 12 * 60] = d_mpipo[: 12 * 60] + od_mpipo

        d_demand[: 12 * 60] = d_demand[: 12 * 60] + od_demand

        d_mdemand[: 12 * 60] = d_mdemand[: 12 * 60] + od_mdemand

        d_power[: 12 * 60] = d_power[: 12 * 60] + od_power

        d_mpower[: 12 * 60] = d_mpower[: 12 * 60] + od_mpower

        # append to data array

        pipo = np.concatenate((pipo, d_pipo[: 24 * 60]), axis=0)

        mpipo = np.concatenate((mpipo, d_mpipo[: 24 * 60]), axis=0)

        demand = np.concatenate((demand, d_demand[: 24 * 60]), axis=0)

        mdemand = np.concatenate((mdemand, d_mdemand[: 24 * 60]), axis=0)

        power = np.concatenate((power, d_power[: 24 * 60]), axis=0)

        mpower = np.concatenate((mpower, d_mpower[: 24 * 60]), axis=0)

        # save over leap to next day

        od_pipo = d_pipo[-12 * 60 :]

        od_mpipo = d_mpipo[-12 * 60 :]

        od_demand = d_demand[-12 * 60 :]

        od_mdemand = d_mdemand[-12 * 60 :]

        od_power = d_power[-12 * 60 :]

        od_mpower = d_mpower[-12 * 60 :]

    # time sampling

    s1a = pd.Series(

        pipo,

        name="pipo",

        index=pd.date_range(

            start=root_now, periods=nr_days * 24 * 60, freq="1min"

        ),

    )

    s1b = pd.Series(

        pipo,

        name="pipo",

        index=pd.date_range(

            start=root_now - pd.Timedelta(1, "d").days,

            periods=nr_days * 24 * 60,

            freq="1min",

        ),

    )

    s2a = pd.Series(

        demand,

        name="demand",

        index=pd.date_range(

            start=root_now, periods=nr_days * 24 * 60, freq="1min"

        ),

    )

    s2b = pd.Series(

        mdemand,

        name="demand",

        index=pd.date_range(

            start=root_now - pd.Timedelta(1, "d").days,

            periods=nr_days * 24 * 60,

            freq="1min",

        ),

    )

    s3a = pd.Series(

        power,

        name="power",

        index=pd.date_range(

            start=root_now, periods=nr_days * 24 * 60, freq="1min"

        ),

    )

    s3b = pd.Series(

        mpower,

        name="power",

        index=pd.date_range(

            start=root_now - pd.Timedelta(1, "d").days,

            periods=nr_days * 24 * 60,

            freq="1min",

        ),

    )

    s1a = s1a.resample(sampling_time).max()

    s1b = s1b.resample(sampling_time).max()

    s2a = s2a.resample(sampling_time).sum()

    s2b = s2b.resample(sampling_time).sum()

    s3a = s3a.resample(sampling_time).mean()

    s3b = s3b.resample(sampling_time).mean()

    # build data frames

    df_f = (

        s1a.to_frame()

        .merge(s2a.to_frame(), left_index=True, right_index=True)

        .merge(s3a.to_frame(), left_index=True, right_index=True)

    )

    df_m = (

        s1b.to_frame()

        .merge(s2b.to_frame(), left_index=True, right_index=True)

        .merge(s3b.to_frame(), left_index=True, right_index=True)

    )

    return df_m, df_f

d = {'col1': [323, 21,32], 'col2': [3, 4, 732], 'col3': [5,6, 8]}
power = []
i = 0
while (i < 4):
    power.append(random.randint(0, 14))
    i += 1
print(power)
task_instance = TaskSimEvCharging(10, 60, 5, 50, 0, 100, power)
df = pd.DataFrame(data=d)
print(_simulate_ev_forecast(df, task_instance))