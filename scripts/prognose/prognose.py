from pandas import DataFrame, Timestamp, Timedelta, date_range, concat, Series
from numpy import zeros, array, ceil, concatenate

from numpy.random import seed

from random import choices

from chaospy import Normal, Trunc
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


def simulate_ev_forecast(df: DataFrame, cfg: TaskSimEvCharging) -> DataFrame:
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

    dec_charge = Normal(0.5, 0.1)

    dec_start = Normal(

        cfg.min_start + (cfg.max_start - cfg.min_start) * 0.5,

        (cfg.max_start - cfg.min_start) * 0.15,

    )

    noise = Normal(0, 0.3)

    dec_duration = Normal(

        cfg.min_duration + (cfg.max_duration - cfg.min_duration) * 0.5,

        (cfg.max_duration - cfg.min_duration) * 0.15,

    )

    dec_demand = Normal(

        cfg.min_demand + (cfg.max_demand - cfg.min_demand) * 0.5,

        (cfg.max_demand - cfg.min_demand) * 0.15,

    )

    # generate ev event for one charging points

    plans = []

    nr_days = (

        df.index.unique().size

    )  # get number of day's indirect over input time series index

    seed(

        Timestamp.utcnow().dayofyear

    )  # initialization of seed by day number of year

    # make decisions for each day

    # takes charing place or not

    charge_dec = dec_charge.sample(nr_days) <= 0.8

    # how many minutes after midnight starts the charging process

    start_dec = dec_start.sample(nr_days)  # for forecast

    root_now = 15

    mstart_dec = noise.sample(nr_days)  # for measurement (-1d)

    # how long is the charging process

    duration_dec = dec_duration.sample(nr_days)  # for forecast

    mduration_dec = noise.sample(nr_days)  # fore measurement

    # how how much energy is needed
    trunc_dist = Trunc(dec_demand, lower=0)
    demand_dec = trunc_dist.sample(nr_days)  # for forecast

    mdemand_dec = noise.sample(nr_days)  # for measurement

    # at what power level takes the charging place

    power_dec = choices(cfg.power, k=nr_days)

    # put decissions together in an event list

    for i in range(nr_days):
        plans.append(

            EvChargingPlan(

                charge=charge_dec[i],

                start=int(start_dec[i]) if charge_dec[i] else 0,

                mstart=int(start_dec[i] + mstart_dec[i] * 30)

                if charge_dec[i]

                else 0,

                duration=int(duration_dec[i]) if charge_dec[i] else 0,

                mduration=int(duration_dec[i] + mduration_dec[i] * 60)

                if charge_dec[i]

                else 0,

                demand=demand_dec[i] if charge_dec[i] else 0,

                mdemand=demand_dec[i] + mdemand_dec[i] * 5 if charge_dec[i] else 0,

                power=power_dec[i] if charge_dec[i] else 0,

            )

        )

    # build result time series

    pipo = array([])

    mpipo = array([])

    demand = array([])

    mdemand = array([])

    power = array([])

    mpower = array([])

    od_pipo = zeros(12 * 60)

    od_mpipo = zeros(12 * 60)

    od_demand = zeros(12 * 60)

    od_mdemand = zeros(12 * 60)

    od_power = zeros(12 * 60)

    od_mpower = zeros(12 * 60)

    for item in plans:

        d_pipo = zeros(36 * 60)

        d_mpipo = zeros(36 * 60)

        d_demand = zeros(36 * 60)

        d_mdemand = zeros(36 * 60)

        d_power = zeros(36 * 60)

        d_mpower = zeros(36 * 60)

        if item.charge:

            d_pipo[item.start - 1: item.start - 1 + item.duration] = 1

            d_mpipo[item.mstart - 1: item.mstart - 1 + item.mduration] = 1

            d_demand[item.start - 1: item.start - 1 + item.duration] = (

                    item.demand / item.duration

            )

            d_mdemand[item.mstart - 1: item.mstart - 1 + item.mduration] = (

                    item.mdemand / item.mduration

            )

            # power shuts down, after demand is full filled (in minute steps)
            duration = int(ceil(item.demand / item.power * 60))
            if item.duration < duration:
                duration = item.duration
            d_power[item.start - 1: item.start - 1 + duration] = item.power

            # measured power

            for k in range(item.mstart - 1, 2159):
                seed(Timestamp.utcnow().dayofyear)
                d_mpower[k] = item.power - abs(noise.sample(1)) * 0.1 * item.power

        # add history

        d_pipo[: 12 * 60] = d_pipo[: 12 * 60] + od_pipo

        d_mpipo[: 12 * 60] = d_mpipo[: 12 * 60] + od_mpipo

        d_demand[: 12 * 60] = d_demand[: 12 * 60] + od_demand

        d_mdemand[: 12 * 60] = d_mdemand[: 12 * 60] + od_mdemand

        d_power[: 12 * 60] = d_power[: 12 * 60] + od_power

        d_mpower[: 12 * 60] = d_mpower[: 12 * 60] + od_mpower

        # append to data array

        pipo = concatenate((pipo, d_pipo[: 24 * 60]), axis=0)

        mpipo = concatenate((mpipo, d_mpipo[: 24 * 60]), axis=0)

        demand = concatenate((demand, d_demand[: 24 * 60]), axis=0)

        mdemand = concatenate((mdemand, d_mdemand[: 24 * 60]), axis=0)

        power = concatenate((power, d_power[: 24 * 60]), axis=0)

        mpower = concatenate((mpower, d_mpower[: 24 * 60]), axis=0)

        # save over leap to next day

        od_pipo = d_pipo[-12 * 60:]

        od_mpipo = d_mpipo[-12 * 60:]

        od_demand = d_demand[-12 * 60:]

        od_mdemand = d_mdemand[-12 * 60:]

        od_power = d_power[-12 * 60:]

        od_mpower = d_mpower[-12 * 60:]

    # time sampling

    s1a = Series(

        pipo,

        name="pipo",

        index=date_range(

            start=root_now, periods=nr_days * 24 * 60, freq="1min"

        ),

    )

    s1b = Series(

        pipo,

        name="pipo",

        index=date_range(

            start=root_now,

            periods=nr_days * 24 * 60,

            freq="1min",

        ),

    )

    s2a = Series(

        demand,

        name="demand",

        index=date_range(

            start=root_now, periods=nr_days * 24 * 60, freq="1min"

        ),

    )

    s2b = Series(

        mdemand,

        name="demand",

        index=date_range(

            start=root_now,

            periods=nr_days * 24 * 60,

            freq="1min",

        ),

    )

    s3a = Series(

        power,

        name="power",

        index=date_range(

            start=root_now, periods=nr_days * 24 * 60, freq="1min"

        ),

    )

    s3b = Series(

        mpower,

        name="power",

        index=date_range(

            start=root_now,

            periods=nr_days * 24 * 60,

            freq="1min",

        ),

    )

    sampling_time = '20D'
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

    # return only pipo

    return df_f

