from brokers import SimulationBroker
import matplotlib.pyplot as plt
import pandas as pd
import datetime

class OandaEnvironment:

    def __init__(self, agent_class, instrument, conf_path="./oanda_demo.conf", lookback_depth=11*4, window_size=24*62, reach_level=0.25):

        self.lookback_depth = lookback_depth
        self.window_size = window_size
        self.reach_level = reach_level
        self.simulation_broker = SimulationBroker()

        self.agent = agent_class(self, self)

        self.instrument = str.upper(instrument)

        with open(os.path.expanduser(conf_path)) as f:
            y = yaml.load(f, Loader=yaml.FullLoader)
            self.account_id = y['accounts'][str.lower(self.instrument)]
            self.api = v20.Context(y['hostname'], y['port'], y['ssl'], 'demo100', token=y['token'], datetime_format='UNIX')
            self.streaming_api = v20.Context(y['streaming_hostname'], y['port'], y['ssl'], 'demo100', token=y['token'], datetime_format='UNIX')

        # try:
        #     os.mkdir("./oanda_db")
        # except Exception as e:
        #     pass

        # self.db = sql.connect("./oanda_db/%s.db"%str.lower(self.instrument))

        self.db.execute("""CREATE TABLE IF NOT EXISTS candles
                (timestamp INTEGER PRIMARY KEY NOT NULL,
                 bid_open REAL,
                 bid_high REAL,
                 bid_low REAL,
                 bid_close REAL,
                 ask_open REAL,
                 ask_high REAL,
                 ask_low REAL,
                 ask_close REAL,
                 volume INTEGER)
                """)

        self.db.execute("""CREATE TABLE IF NOT EXISTS ticks
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER  NOT NULL,
                    bid REAL, ask REAL);""")

        self.db.commit()

        self.load_data()

        self.update_candles()

        self.agent.compute_pitchfork(self.past_data)

        self.precision = len(str(self.past_data.iloc[-1]['bid_low']).split('.')[-1])

        self.current_candle = pd.DataFrame()

    def load_data(self):
        # Need to fetch just the window size and then offset the df index by the length of the data, in the meantime we load all data
        # self.past_data = pd.read_sql("SELECT * FROM (SELECT * FROM candles ORDER BY timestamp desc LIMIT %i) ORDER BY timestamp asc;"%self.window_size, self.db)

        self.past_data = pd.read_sql("SELECT * FROM candles ORDER BY timestamp asc;", self.db)

        self.past_data['date'] = pd.to_datetime(self.past_data['timestamp'], unit="ms", origin="unix")

        self.past_data = self.past_data.set_index('timestamp')
        self.past_data.reset_index(inplace=True)
        self.step_i = len(self.past_data) + 1

    def account(self):
        return self.api.account.get(self.account_id)

    # def instruments(self):
    #     response = self.api.account.instruments(self.account_id)
    #     instruments = response.get('instruments')
    #     return [i.name for i in instruments]

    # def quantity_sell(self):
    #     response = self.api.pricing.get(self.account_id, instruments=self.instrument, includeUnitsAvailable=True)
    #     price = response.get("prices", 200)[-1]
    #     short = price.unitsAvailable.default.short
    #     return short

    # def quantity_buy(self):
    #     response = self.api.pricing.get(self.account_id, instruments=self.instrument, includeUnitsAvailable=True)
    #     price = response.get("prices", 200)[-1]
    #     long = price.unitsAvailable.default.long
    #     return long

    def capital(self):
        return self.account().get("account").balance

    def equity(self):
        return self.account().get("account").NAV

    # def trade_open(self):
    #     return len(self.simulation_broker.opened_positions) > 0
    #     # return self.account().get("account").openTradeCount > 0

    def start(self):
        while True:
            try:
                response = self.streaming_api.pricing.stream(self.account_id, snapshot=True, instruments=self.instrument)
                for msg_type, msg in response.parts():
                    if msg_type == 'pricing.ClientPrice' and msg.tradeable:
                        self.on_price_update(msg.closeoutBid, msg.closeoutAsk, int(float(msg.time)*1000))
            except Exception as e:
                print(e)

    def on_price_update(self, bid, ask, timestamp):
        tick_state = {'bid_high': float(bid), 'bid_low': float(bid), 'ask_high': float(ask), 'ask_low': float(ask), 'timestamp': int(timestamp)}
        # print(state)
        data = pd.DataFrame([[int(timestamp), bid, ask]], columns=['timestamp', 'bid', 'ask'])
        data.to_sql('ticks', con=self.db, method='multi', if_exists='append', index=False)
        self.db.commit()

        self.update_candles(end_time=int(timestamp))

        self.current_candle = self.current_candle.append(tick_state, ignore_index=True)

        tick_snapshot = {'bid_high': max(self.current_candle['bid_high']),
                         'bid_low': min(self.current_candle['bid_low']),
                         'ask_high': max(self.current_candle['ask_high']),
                         'ask_low': min(self.current_candle['ask_low']),
                         'bid_close': bid, 'ask_close': ask,
                         'timestamp': int(timestamp)}

        tick_state['step'] = self.step_i
        self.simulation_broker.evaluate_trades(tick_state)
        self.agent.trade(self.past_data.append(tick_snapshot, ignore_index=True), tick_state)

    def update_candles(self, end_time=None):
        # months = datetime.timedelta(days=31)
        # hours = datetime.timedelta(hours=1)
        # start = (datetime.datetime.now() - 4 * months)

        minutes = datetime.timedelta(minutes=1)
        start = (datetime.datetime.now(tz=tz.UTC) - 3 * self.window_size * minutes)
        end = datetime.datetime.now(tz=tz.UTC) - minutes

        if len(self.past_data) > 0:
            start = datetime.datetime.fromtimestamp(self.past_data.tail(1)['timestamp'].values[0]/1000, tz=tz.UTC)

        if end_time:
            end = datetime.datetime.fromtimestamp(end_time/1000, tz=tz.UTC) - minutes

        if end < start + minutes:
            return

        # print(start, end)

        end = end.timestamp()
        start = start.timestamp()

        # granularity = 'H1'
        granularity = 'M1'
        raw = self.api.instrument.candles(
            instrument=self.instrument,
            fromTime=start, toTime=end,
            granularity=granularity, price='BA')
        dbcolumns = ['timestamp', 'bid_open', 'bid_high', 'bid_low', 'bid_close', 'ask_open', 'ask_high', 'ask_low', 'ask_close', 'volume']
        data = pd.DataFrame(columns=dbcolumns)
        candles = raw.get('candles')
        for candle in candles:
            if not len(self.db.execute("SELECT * FROM candles WHERE timestamp=%i"%int(float(candle.time) * 1000)).fetchall()) > 0 :
                row = pd.DataFrame([[int(float(candle.time) * 1000), candle.bid.o, candle.bid.h, candle.bid.l, candle.bid.c, candle.ask.o, candle.ask.h, candle.ask.l, candle.ask.c, candle.volume]], columns=dbcolumns)
                data = data.append(row)
        if len(data) > 0:
            data.to_sql('candles', con=self.db, method='multi', if_exists='append', index=False, chunksize=100)
            self.db.commit()
            self.load_data()
            # New completed candle
            self.current_candle = pd.DataFrame()
            self.agent.compute_pitchfork(self.past_data)

    def open_trade(self, stop_loss, take_profit, quantity):
        sl_details = StopLossDetails(price=self.format_price(stop_loss))
        tp_details = TakeProfitDetails(price=self.format_price(take_profit))
        response = self.api.order.market(self.account_id, instrument=self.instrument, units=int(quantity),
                                         stopLossOnFill=sl_details, takeProfitOnFill=tp_details)
        print('open trade on', self.instrument, quantity, self.format_price(stop_loss), self.format_price(take_profit))
        return 'orderFillTransaction' in response.body
        # return 'orderCreateTransaction' in response.body and 'orderCancelTransaction' not in response.body

    def open_buy(self, state, stop_loss, take_profit, price_to_buy, quantity):
        response = self.open_trade(stop_loss, take_profit, quantity)
        if response:
            self.simulation_broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity)

    def open_sell(self, state, stop_loss, take_profit, price_to_sell, quantity):
        response = self.open_trade(stop_loss, take_profit, - quantity)
        if response:
            self.simulation_broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity)

    def format_price(self, price):
        return ("{:.%if}"%self.precision).format(float(price))
