import h5py
import pandas as pd
import os


def parse_timestamp(ts):
    ts = str(ts)
    if len(ts) == 17:
        year = int(ts[0:4])
        month = int(ts[4:6])
        day = int(ts[6:8])
        hour = int(ts[8:10])
        minute = int(ts[10:12])
        second = int(ts[12:14])
        millisecond = int(ts[14:17])
        microsecond = millisecond * 1000

        return pd.Timestamp(year, month, day, hour, minute, second, microsecond)
    else:
        return pd.NaT
    
def vwap(df, level=10, _type="all"):
    if _type == "ask":
        pv = df.apply(lambda row: sum([row[f"AskPrice{str(i)}"]*row[f"AskVolume{str(i)}"] for i in range(1,level+1)]),axis=1)
        v = df.apply(lambda row: sum([row[f"AskVolume{str(i)}"] for i in range(1,level+1)]),axis=1)
    if _type == "bid":
        pv = df.apply(lambda row: sum([row[f"BidPrice{str(i)}"]*row[f"BidVolume{str(i)}"] for i in range(1,level+1)]),axis=1)
        v = df.apply(lambda row: sum([row[f"BidVolume{str(i)}"] for i in range(1,level+1)]),axis=1)
    if _type == "all":
        askvwap, askv = vwap(df, level, _type="ask")
        bidvwap, bidv = vwap(df, level, _type="bid")
        pv, v = askvwap*askv + bidvwap*bidv, askv + bidv
    return pv/v, v

class PPL:
    def __init__(self, base_path="./interview"):
        self.base_path = base_path

    def profiler(self, dayprofiler, kwargs={}, id="002521", start_date="20220601", end_date="20221013"):
        dates = list(map(lambda x: x.strftime("%Y%m%d"), pd.date_range(start=start_date, end=end_date).tolist()))
        res = []
        res_dates = []
        for date in dates:
            if os.path.exists(f'{self.base_path}/{id}_{date}.h5'):
                res_dates.append(date)
                with h5py.File(f'{self.base_path}/{id}_{date}.h5', 'r') as f:
                    res.append(dayprofiler(f, **kwargs))
        return pd.concat(res, keys=res_dates)
    
    @staticmethod
    def get_basic(f, freq="1T"):
        df = pd.DataFrame({
            k: f[k][:] for k in f.keys()
        })
        df["MidPrice"] = (df["AskPrice1"] * df["AskVolume1"] + df["BidPrice1"] * df["BidVolume1"]) / (df["AskVolume1"] + df["BidVolume1"])
        df["Time"] = df["DataTime"].apply(parse_timestamp)
        df.set_index("Time", inplace=True)
        resampled = df.resample(freq).agg(
            {
                "MidPrice" : ["first", "last"],
                "Volume" : ["first", "last"],
            }
        )
        res = pd.DataFrame({
            "open" : resampled["MidPrice"]["first"],
            "close" : resampled["MidPrice"]["last"],
            "volume": resampled["Volume"]["last"] - resampled["Volume"]["first"]
        })
        return res
    
ppl = PPL()
ppl.profiler(PPL.get_basic, kwargs={"freq":"1min"})