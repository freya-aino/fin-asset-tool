from numpy.random.mtrand import shuffle
from history import load_price
from abc import ABC,  abstractmethod
import numpy as np
import time
from copy import deepcopy
import matplotlib.pyplot as plt
import torch as T
import torch.nn as nn
import ta


class SimulationTrade:
    
    def __init__(self, direction, datetime_issued, open_price, stop_loss, take_profit, quantity, global_offset):
        
        self.direction = direction
        self.datetime_issued = datetime_issued
        self.datetime_opened = None
        self.datetime_closed = None
        self.open_price = open_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.quantity = quantity
        self.is_open = False
        self.total_gain = 0.
        self.global_offset = global_offset
    
    def check(self, current_bid_high, current_bid_low, current_ask_high, current_ask_low, current_timestamp):

        if not self.is_open:

            if self.direction == "buy" and current_ask_high >= self.open_price and current_ask_low <= self.open_price:

                if current_bid_high >= self.take_profit or current_bid_low <= self.stop_loss:
                    return "invalid"
                else:
                    self.is_open         = True
                    self.datetime_opened = current_timestamp
                    return "opened"

            elif self.direction == "sell" and current_bid_high >= self.open_price and current_bid_low <= self.open_price:

                if current_ask_low <= self.take_profit or current_ask_high >= self.stop_loss:
                    return "invalid"
                else:
                    self.is_open         = True
                    self.datetime_opened = current_timestamp
                    return "opened"

        else:

            if self.direction == "buy":

                if current_bid_high >= self.take_profit and current_bid_low <= self.stop_loss:
                    return "invalid"

                elif current_bid_high >= self.take_profit:
                    self.is_open         = False
                    self.datetime_closed = current_timestamp
                    self.total_gain      = abs(current_bid_high - self.open_price) * self.quantity
                    return "closed"

                elif current_bid_low <= self.stop_loss:
                    self.is_open         = False
                    self.datetime_closed = current_timestamp
                    self.total_gain      = -1 * abs(current_bid_high - self.open_price) * self.quantity
                    return "closed"


            elif self.direction == "sell":

                if current_ask_low <= self.take_profit and current_ask_high >= self.stop_loss:
                    return "invalid"
            
                elif current_ask_low <= self.take_profit:
                    self.is_open         = False
                    self.datetime_closed = current_timestamp
                    self.total_gain      = abs(current_ask_low - self.open_price) * self.quantity
                    return "closed"

                elif current_ask_high >= self.stop_loss:
                    self.is_open         = False
                    self.datetime_closed = current_timestamp
                    self.total_gain      = -1 * abs(current_ask_high - self.open_price) * self.quantity
                    return "closed"

# ------------------------------------------------------------------------------

class Broker(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def create_trade(self, direction, open_price, stop_loss, take_profit, issue_timestamp, leverage, quantity=1.0):
        pass

class SimulationBroker(Broker):

    def __init__(self, max_number_of_parallel_trades_opened):
        self.max_number_of_parallel_trades_opened = max_number_of_parallel_trades_opened
        self.valid_non_finished_trades = []
        self.finished_trades = []
        self.invalid_trades = []
        pass
    
    def create_trade(self, trade): # direction, open_price, stop_loss, take_profit, issue_timestamp, leverage, quantity=1.0):
        if len(self.valid_non_finished_trades) < self.max_number_of_parallel_trades_opened:
            self.valid_non_finished_trades.append(trade)
    
    def check_trades(self, 
                     current_bid_high, current_bid_low,
                     current_ask_high, current_ask_low,
                     current_timestamp):

        new_valid_non_finished_trades = []

        for i in range(len(self.valid_non_finished_trades)):
            signal = self.valid_non_finished_trades[i].check(current_bid_high = current_bid_high,
                                                             current_bid_low = current_bid_low,
                                                             current_ask_high = current_ask_high,
                                                             current_ask_low = current_ask_low,
                                                             current_timestamp = current_timestamp)
            
            if signal == "invalid":
                self.invalid_trades.append(self.valid_non_finished_trades[i])
            elif signal == "closed":
                self.finished_trades.append(self.valid_non_finished_trades[i])
            else:
                new_valid_non_finished_trades.append(self.valid_non_finished_trades[i])

        self.valid_non_finished_trades = new_valid_non_finished_trades

# ------------------------------------------------------------------------------

class Agent(ABC):

    @abstractmethod
    def __init__(self, init_capital):
        self.init_capital = init_capital
        self.capital = init_capital

    @abstractmethod
    def trade(self, past_window, state):
        # decides whether to open a trade
        pass

class DLAgent(Agent):

    def __init__(self, init_capital, lookback_depth, prediction_depth, loss_threshold, lr, hidden_size = 10, loss_moving_average = 20,  max_number_of_training_examples = 30):
        super().__init__(init_capital)
        
        self.trades          = []
        self.finished_trades = []
        self.invalid_trades  = []

        self.training_examples = []

        self.Q_lstm = nn.LSTM(36, hidden_size, batch_first = True)
        self.K_lstm = nn.LSTM(36, hidden_size, batch_first = True)
        self.V_lstm = nn.LSTM(36, hidden_size, batch_first = True)
        self.Q_ln = nn.LayerNorm(hidden_size)
        self.K_ln = nn.LayerNorm(hidden_size)
        self.V_ln = nn.LayerNorm(hidden_size)
        self.out_lstm = nn.LSTM(hidden_size, 2 * prediction_depth, batch_first = True)
        self.bn_w = T.rand([1], dtype=T.float32, requires_grad=True)
        self.bn_b = T.rand([1], dtype=T.float32, requires_grad=True)
        self.model_loss = nn.MSELoss()
        self.model_opt = T.optim.Adam([*self.Q_ln.parameters(), *self.K_ln.parameters(), *self.V_ln.parameters(), self.bn_w, self.bn_w, *self.Q_lstm.parameters(), *self.K_lstm.parameters(), *self.V_lstm.parameters(), *self.out_lstm.parameters()], lr = lr)

        self.batch_moving_norm = None

        self.ma_loss = np.array([999. for _ in range(loss_moving_average)])

        self.loss_thr = loss_threshold
        self.prediction_depth = prediction_depth
        self.lookback_depth = lookback_depth
        self.max_number_of_training_examples = max_number_of_training_examples

        self.leverage = 20


    def model_forward(self, x):
        
        x = x.permute(0, 2, 1)

        Q = self.Q_ln(self.Q_lstm(x)[0])
        K = self.K_ln(self.K_lstm(x)[0]).permute(0, 2, 1)
        V = self.V_ln(self.V_lstm(x)[0])

        ATT = T.bmm(Q, K)
        out = T.bmm(ATT, V)

        # if x.shape[0] > 1:
        #     if self.batch_moving_norm == None:
        #         self.batch_moving_norm = {
        #             "std": out.std(dim=0).detach(),
        #             "mean": out.mean(dim=0).detach()
        #         }
        #     else:
        #         self.batch_moving_norm["std"]  = (self.batch_moving_norm["std"] + out.std().detach()) / 2
        #         self.batch_moving_norm["mean"] = (self.batch_moving_norm["mean"] + out.mean().detach()) / 2
            
        #     out = (out - self.batch_moving_norm["mean"]) / self.batch_moving_norm["std"]
        #     out = out * self.bn_w + self.bn_b

        y_ = self.out_lstm(out)[1][1].reshape(x.shape[0], 2, self.prediction_depth)

        return y_


    def train(self):

        shuffle(self.training_examples)

        if len(self.training_examples) > self.max_number_of_training_examples:
            self.training_examples = self.training_examples[:-1]

        if len(self.training_examples) == self.max_number_of_training_examples:

            x = T.stack([t["x"] for t in self.training_examples])
            y = T.stack([t["y"] for t in self.training_examples])
            y_ = self.model_forward(x)
            L = self.model_loss(y_, y)

            self.ma_loss = np.concatenate([self.ma_loss[1:], [L.detach().item()]])

            self.model_opt.zero_grad()
            L.backward()
            self.model_opt.step()

        return None


    def trade(self, past_window, current_state, global_offset):

        bb = ta.volatility.BollingerBands(past_window["bid_close"], window = 20, window_dev = 2)
        dc = ta.volatility.DonchianChannel(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 20, offset = 0)
        kel = ta.volatility.KeltnerChannel(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 20, window_atr = 10)
        adx = ta.trend.ADXIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 14)
        aroon = ta.trend.AroonIndicator(past_window["bid_close"], window = 25)
        kst = ta.trend.KSTIndicator(past_window["bid_close"], roc1 = 10, roc2 = 15, roc3 = 20, roc4 = 30, window1 = 10, window2 = 20, window3 = 10, window4 = 15, nsig = 9)
        vi = ta.trend.VortexIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 14)
        
        past_window["ATR"] = ta.volatility.AverageTrueRange(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 14).average_true_range()
        past_window["BB_HIGH"] = bb.bollinger_hband()
        past_window["BB_LOW"] = bb.bollinger_lband()
        past_window["DC_HIGH"] = dc.donchian_channel_hband()
        past_window["DC_LOW"] = dc.donchian_channel_lband()
        past_window["KEL_HIGH"] = kel.keltner_channel_hband()
        past_window["KEL_LOW"] = kel.keltner_channel_lband()
        past_window["ULCER"] = ta.volatility.UlcerIndex(past_window["bid_close"], window = 14).ulcer_index()
        past_window["SMA"] = ta.trend.SMAIndicator(past_window["bid_close"], window = 14).sma_indicator()
        past_window["ADX"] = adx.adx()
        past_window["ADX_NEG"] = adx.adx_neg()
        past_window["ADX_POS"] = adx.adx_pos()
        past_window["AROON"] = aroon.aroon_indicator()
        past_window["AROON_UP"] = aroon.aroon_up()
        past_window["AROON_DOWN"] = aroon.aroon_down()
        past_window["CCI"] = ta.trend.CCIIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 20).cci()
        past_window["DPO"] = ta.trend.DPOIndicator(past_window["bid_close"], window = 20).dpo()
        past_window["EMA"] = ta.trend.EMAIndicator(past_window["bid_close"], window = 14).ema_indicator()
        past_window["ICHIMOKU_BASE"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_base_line()
        past_window["ICHIMOKU_CONV"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_conversion_line()
        past_window["ICHIMOKU_A"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_a()
        past_window["ICHIMOKU_B"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_b()
        past_window["KST"] = kst.kst()
        past_window["KST_SIG"] = kst.kst_sig()
        past_window["MACD"] = ta.trend.MACD(past_window["bid_close"], window_slow = 26, window_fast = 12, window_sign = 9).macd()
        past_window["MACD_SIG"] = ta.trend.MACD(past_window["bid_close"], window_slow = 26, window_fast = 12, window_sign = 9).macd_signal()
        past_window["MI"] = ta.trend.MassIndex(past_window["bid_high"], past_window["bid_low"], window_slow = 9, window_fast = 25).mass_index()
        past_window["PSAR"] = ta.trend.PSARIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], step = 0.02, max_step = 0.2).psar()
        past_window["STC"] = ta.trend.STCIndicator(past_window["bid_close"], window_slow = 50, window_fast = 23, cycle = 10, smooth1 = 3, smooth2 = 3).stc()
        past_window["TRIX"] = ta.trend.TRIXIndicator(past_window["bid_close"], window = 15).trix()
        past_window["VI_NEG"] = vi.vortex_indicator_neg()
        past_window["VI_POS"] = vi.vortex_indicator_pos()

        ATR = T.tensor(np.array(past_window["ATR"][-(self.lookback_depth):]) - np.array(past_window["ATR"][-(self.lookback_depth+1):-1]))
        BB_HIGH = T.tensor(np.array(past_window["BB_HIGH"][-(self.lookback_depth):]) - np.array(past_window["BB_HIGH"][-(self.lookback_depth+1):-1]))
        BB_LOW = T.tensor(np.array(past_window["BB_LOW"][-(self.lookback_depth):]) - np.array(past_window["BB_LOW"][-(self.lookback_depth+1):-1]))
        DC_HIGH = T.tensor(np.array(past_window["DC_HIGH"][-(self.lookback_depth):]) - np.array(past_window["DC_HIGH"][-(self.lookback_depth+1):-1]))
        DC_LOW = T.tensor(np.array(past_window["DC_LOW"][-(self.lookback_depth):]) - np.array(past_window["DC_LOW"][-(self.lookback_depth+1):-1]))
        KEL_HIGH = T.tensor(np.array(past_window["KEL_HIGH"][-(self.lookback_depth):]) - np.array(past_window["KEL_HIGH"][-(self.lookback_depth+1):-1]))
        KEL_LOW = T.tensor(np.array(past_window["KEL_LOW"][-(self.lookback_depth):]) - np.array(past_window["KEL_LOW"][-(self.lookback_depth+1):-1]))
        ULCER = T.tensor(np.array(past_window["ULCER"][-(self.lookback_depth):]) - np.array(past_window["ULCER"][-(self.lookback_depth+1):-1]))
        SMA = T.tensor(np.array(past_window["SMA"][-(self.lookback_depth):]) - np.array(past_window["SMA"][-(self.lookback_depth+1):-1]))
        ADX = T.tensor(np.array(past_window["ADX"][-(self.lookback_depth):]) - np.array(past_window["ADX"][-(self.lookback_depth+1):-1]))
        ADX_NEG = T.tensor(np.array(past_window["ADX_NEG"][-(self.lookback_depth):]) - np.array(past_window["ADX_NEG"][-(self.lookback_depth+1):-1]))
        ADX_POS = T.tensor(np.array(past_window["ADX_POS"][-(self.lookback_depth):]) - np.array(past_window["ADX_POS"][-(self.lookback_depth+1):-1]))
        AROON = T.tensor(np.array(past_window["AROON"][-(self.lookback_depth):]) - np.array(past_window["AROON"][-(self.lookback_depth+1):-1]))
        AROON_UP = T.tensor(np.array(past_window["AROON_UP"][-(self.lookback_depth):]) - np.array(past_window["AROON_UP"][-(self.lookback_depth+1):-1]))
        AROON_DOWN = T.tensor(np.array(past_window["AROON_DOWN"][-(self.lookback_depth):]) - np.array(past_window["AROON_DOWN"][-(self.lookback_depth+1):-1]))
        CCI = T.tensor(np.array(past_window["CCI"][-(self.lookback_depth):]) - np.array(past_window["CCI"][-(self.lookback_depth+1):-1]))
        DPO = T.tensor(np.array(past_window["DPO"][-(self.lookback_depth):]) - np.array(past_window["DPO"][-(self.lookback_depth+1):-1]))
        EMA = T.tensor(np.array(past_window["EMA"][-(self.lookback_depth):]) - np.array(past_window["EMA"][-(self.lookback_depth+1):-1]))
        ICHIMOKU_BASE = T.tensor(np.array(past_window["ICHIMOKU_BASE"][-(self.lookback_depth):]) - np.array(past_window["ICHIMOKU_BASE"][-(self.lookback_depth+1):-1]))
        ICHIMOKU_CONV = T.tensor(np.array(past_window["ICHIMOKU_CONV"][-(self.lookback_depth):]) - np.array(past_window["ICHIMOKU_CONV"][-(self.lookback_depth+1):-1]))
        ICHIMOKU_A = T.tensor(np.array(past_window["ICHIMOKU_A"][-(self.lookback_depth):]) - np.array(past_window["ICHIMOKU_A"][-(self.lookback_depth+1):-1]))
        ICHIMOKU_B = T.tensor(np.array(past_window["ICHIMOKU_B"][-(self.lookback_depth):]) - np.array(past_window["ICHIMOKU_B"][-(self.lookback_depth+1):-1]))
        KST = T.tensor(np.array(past_window["KST"][-(self.lookback_depth):]) - np.array(past_window["KST"][-(self.lookback_depth+1):-1]))
        KST_SIG = T.tensor(np.array(past_window["KST_SIG"][-(self.lookback_depth):]) - np.array(past_window["KST_SIG"][-(self.lookback_depth+1):-1]))
        MACD = T.tensor(np.array(past_window["MACD"][-(self.lookback_depth):]) - np.array(past_window["MACD"][-(self.lookback_depth+1):-1]))
        MACD_SIG = T.tensor(np.array(past_window["MACD_SIG"][-(self.lookback_depth):]) - np.array(past_window["MACD_SIG"][-(self.lookback_depth+1):-1]))
        MI = T.tensor(np.array(past_window["MI"][-(self.lookback_depth):]) - np.array(past_window["MI"][-(self.lookback_depth+1):-1]))
        PSAR = T.tensor(np.array(past_window["PSAR"][-(self.lookback_depth):]) - np.array(past_window["PSAR"][-(self.lookback_depth+1):-1]))
        STC = T.tensor(np.array(past_window["STC"][-(self.lookback_depth):]) - np.array(past_window["STC"][-(self.lookback_depth+1):-1]))
        TRIX = T.tensor(np.array(past_window["TRIX"][-(self.lookback_depth):]) - np.array(past_window["TRIX"][-(self.lookback_depth+1):-1]))
        VI_NEG = T.tensor(np.array(past_window["VI_NEG"][-(self.lookback_depth):]) - np.array(past_window["VI_NEG"][-(self.lookback_depth+1):-1]))
        VI_POS = T.tensor(np.array(past_window["VI_POS"][-(self.lookback_depth):]) - np.array(past_window["VI_POS"][-(self.lookback_depth+1):-1]))
        x_bid_high = T.tensor(np.array(past_window["bid_high"][-(self.lookback_depth):]) - np.array(past_window["bid_high"][-(self.lookback_depth+1):-1]))
        x_bid_low  = T.tensor(np.array(past_window["bid_low"][-(self.lookback_depth):]) - np.array(past_window["bid_low"][-(self.lookback_depth+1):-1]))
        x_ask_high = T.tensor(np.array(past_window["ask_high"][-(self.lookback_depth):]) - np.array(past_window["ask_high"][-(self.lookback_depth+1):-1]))
        x_ask_low  = T.tensor(np.array(past_window["ask_low"][-(self.lookback_depth):]) - np.array(past_window["ask_low"][-(self.lookback_depth+1):-1]))

        x = T.stack([x_bid_high, x_bid_low, 
                     x_ask_high, x_ask_low,
                     ATR, BB_HIGH, BB_LOW,
                     DC_HIGH, DC_LOW, KEL_HIGH,
                     KEL_LOW, ULCER, SMA,
                     ADX, ADX_NEG, ADX_POS,
                     AROON, AROON_UP, AROON_DOWN,
                     CCI, DPO, EMA,
                     ICHIMOKU_BASE, ICHIMOKU_CONV,
                     ICHIMOKU_A, ICHIMOKU_B,
                     KST, KST_SIG, MACD,
                     MACD_SIG, MI, PSAR,
                     STC, TRIX, VI_NEG, VI_POS]).to(T.float32)
        x = x[None, :, :]

        y_ = self.model_forward(x)[0, :, :]
        y_ = T.cumsum(y_, dim = 1)

        high = y_[0, :]
        low = y_[1, :]

        print(high[-1])
        print(low[-1])

        if high[-1] <= low[-1]:

            pass

        elif high[-1] > 0 and low[-1] < 0:

            if abs(high[-1]) > abs(low[-1]):

                stop_loss = current_state["ask_high"] - abs(low[-1].detach().item()) - 1
                take_profit = current_state["ask_high"] + abs(high[-1].detach().item()) - 1

                trade = SimulationTrade(direction       = "buy",
                                        datetime_issued = current_state["timestamp"],
                                        open_price      = current_state["ask_high"],
                                        stop_loss       = stop_loss,
                                        take_profit     = take_profit,
                                        quantity        = (self.capital * self.leverage) / current_state["ask_high"],
                                        global_offset   = global_offset)
            else:

                stop_loss = current_state["bid_low"] + abs(high[-1].detach().item()) + 1
                take_profit = current_state["bid_low"] - abs(low[-1].detach().item()) + 1

                trade = SimulationTrade(direction   = "sell",
                                    datetime_issued = current_state["timestamp"],
                                    open_price      = current_state["bid_low"],
                                    stop_loss       = stop_loss,
                                    take_profit     = take_profit,
                                    quantity        = (self.capital * self.leverage) / current_state["bid_low"],
                                    global_offset   = global_offset)
                                    
            return trade
        
        elif high[-1] < 0 and low[-1] < 0:
        
            stop_loss = current_state["bid_low"] + abs(high[-1].detach().item()) + 1
            take_profit = current_state["bid_low"] - abs(low[-1].detach().item()) + 1

            trade = SimulationTrade(direction   = "sell",
                                datetime_issued = current_state["timestamp"],
                                open_price      = current_state["bid_low"],
                                stop_loss       = stop_loss,
                                take_profit     = take_profit,
                                quantity        = (self.capital * self.leverage) / current_state["bid_low"],
                                global_offset   = global_offset)

            return trade

        elif high[-1] > 0 and low[-1] > 0:
    
            stop_loss = current_state["bid_low"] + abs(high[-1].detach().item()) + 1
            take_profit = current_state["bid_low"] - abs(low[-1].detach().item()) + 1

            trade = SimulationTrade(direction       = "buy",
                                    datetime_issued = current_state["timestamp"],
                                    open_price      = current_state["ask_high"],
                                    stop_loss       = stop_loss,
                                    take_profit     = take_profit,
                                    quantity        = (self.capital * self.leverage) / current_state["ask_high"],
                                    global_offset   = global_offset)

            return trade

        return None


    def collect_trade(self, past_window, current_state, global_offset):

        bb = ta.volatility.BollingerBands(past_window["bid_close"], window = 20, window_dev = 2)
        dc = ta.volatility.DonchianChannel(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 20, offset = 0)
        kel = ta.volatility.KeltnerChannel(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 20, window_atr = 10)
        adx = ta.trend.ADXIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 14)
        aroon = ta.trend.AroonIndicator(past_window["bid_close"], window = 25)
        kst = ta.trend.KSTIndicator(past_window["bid_close"], roc1 = 10, roc2 = 15, roc3 = 20, roc4 = 30, window1 = 10, window2 = 20, window3 = 10, window4 = 15, nsig = 9)
        vi = ta.trend.VortexIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 14)
        
        past_window["ATR"] = ta.volatility.AverageTrueRange(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 14).average_true_range()
        past_window["BB_HIGH"] = bb.bollinger_hband()
        past_window["BB_LOW"] = bb.bollinger_lband()
        past_window["DC_HIGH"] = dc.donchian_channel_hband()
        past_window["DC_LOW"] = dc.donchian_channel_lband()
        past_window["KEL_HIGH"] = kel.keltner_channel_hband()
        past_window["KEL_LOW"] = kel.keltner_channel_lband()
        past_window["ULCER"] = ta.volatility.UlcerIndex(past_window["bid_close"], window = 14).ulcer_index()
        past_window["SMA"] = ta.trend.SMAIndicator(past_window["bid_close"], window = 14).sma_indicator()
        past_window["ADX"] = adx.adx()
        past_window["ADX_NEG"] = adx.adx_neg()
        past_window["ADX_POS"] = adx.adx_pos()
        past_window["AROON"] = aroon.aroon_indicator()
        past_window["AROON_UP"] = aroon.aroon_up()
        past_window["AROON_DOWN"] = aroon.aroon_down()
        past_window["CCI"] = ta.trend.CCIIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], window = 20).cci()
        past_window["DPO"] = ta.trend.DPOIndicator(past_window["bid_close"], window = 20).dpo()
        past_window["EMA"] = ta.trend.EMAIndicator(past_window["bid_close"], window = 14).ema_indicator()
        past_window["ICHIMOKU_BASE"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_base_line()
        past_window["ICHIMOKU_CONV"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_conversion_line()
        past_window["ICHIMOKU_A"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_a()
        past_window["ICHIMOKU_B"] = ta.trend.IchimokuIndicator(past_window["bid_high"], past_window["bid_low"], 9, 26, 52).ichimoku_b()
        past_window["KST"] = kst.kst()
        past_window["KST_SIG"] = kst.kst_sig()
        past_window["MACD"] = ta.trend.MACD(past_window["bid_close"], window_slow = 26, window_fast = 12, window_sign = 9).macd()
        past_window["MACD_SIG"] = ta.trend.MACD(past_window["bid_close"], window_slow = 26, window_fast = 12, window_sign = 9).macd_signal()
        past_window["MI"] = ta.trend.MassIndex(past_window["bid_high"], past_window["bid_low"], window_slow = 9, window_fast = 25).mass_index()
        past_window["PSAR"] = ta.trend.PSARIndicator(past_window["bid_high"], past_window["bid_low"], past_window["bid_close"], step = 0.02, max_step = 0.2).psar()
        past_window["STC"] = ta.trend.STCIndicator(past_window["bid_close"], window_slow = 50, window_fast = 23, cycle = 10, smooth1 = 3, smooth2 = 3).stc()
        past_window["TRIX"] = ta.trend.TRIXIndicator(past_window["bid_close"], window = 15).trix()
        past_window["VI_NEG"] = vi.vortex_indicator_neg()
        past_window["VI_POS"] = vi.vortex_indicator_pos()

        ATR = T.tensor(np.array(past_window["ATR"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ATR"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        BB_HIGH = T.tensor(np.array(past_window["BB_HIGH"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["BB_HIGH"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        BB_LOW = T.tensor(np.array(past_window["BB_LOW"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["BB_LOW"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        DC_HIGH = T.tensor(np.array(past_window["DC_HIGH"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["DC_HIGH"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        DC_LOW = T.tensor(np.array(past_window["DC_LOW"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["DC_LOW"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        KEL_HIGH = T.tensor(np.array(past_window["KEL_HIGH"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["KEL_HIGH"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        KEL_LOW = T.tensor(np.array(past_window["KEL_LOW"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["KEL_LOW"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ULCER = T.tensor(np.array(past_window["ULCER"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ULCER"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        SMA = T.tensor(np.array(past_window["SMA"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["SMA"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ADX = T.tensor(np.array(past_window["ADX"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ADX"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ADX_NEG = T.tensor(np.array(past_window["ADX_NEG"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ADX_NEG"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ADX_POS = T.tensor(np.array(past_window["ADX_POS"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ADX_POS"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        AROON = T.tensor(np.array(past_window["AROON"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["AROON"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        AROON_UP = T.tensor(np.array(past_window["AROON_UP"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["AROON_UP"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        AROON_DOWN = T.tensor(np.array(past_window["AROON_DOWN"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["AROON_DOWN"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        CCI = T.tensor(np.array(past_window["CCI"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["CCI"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        DPO = T.tensor(np.array(past_window["DPO"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["DPO"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        EMA = T.tensor(np.array(past_window["EMA"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["EMA"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ICHIMOKU_BASE = T.tensor(np.array(past_window["ICHIMOKU_BASE"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ICHIMOKU_BASE"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ICHIMOKU_CONV = T.tensor(np.array(past_window["ICHIMOKU_CONV"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ICHIMOKU_CONV"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ICHIMOKU_A = T.tensor(np.array(past_window["ICHIMOKU_A"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ICHIMOKU_A"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        ICHIMOKU_B = T.tensor(np.array(past_window["ICHIMOKU_B"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ICHIMOKU_B"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        KST = T.tensor(np.array(past_window["KST"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["KST"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        KST_SIG = T.tensor(np.array(past_window["KST_SIG"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["KST_SIG"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        MACD = T.tensor(np.array(past_window["MACD"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["MACD"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        MACD_SIG = T.tensor(np.array(past_window["MACD_SIG"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["MACD_SIG"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        MI = T.tensor(np.array(past_window["MI"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["MI"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        PSAR = T.tensor(np.array(past_window["PSAR"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["PSAR"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        STC = T.tensor(np.array(past_window["STC"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["STC"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        TRIX = T.tensor(np.array(past_window["TRIX"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["TRIX"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        VI_NEG = T.tensor(np.array(past_window["VI_NEG"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["VI_NEG"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        VI_POS = T.tensor(np.array(past_window["VI_POS"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["VI_POS"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        x_bid_high = T.tensor(np.array(past_window["bid_high"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["bid_high"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        x_bid_low  = T.tensor(np.array(past_window["bid_low"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["bid_low"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        x_ask_high = T.tensor(np.array(past_window["ask_high"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ask_high"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))
        x_ask_low  = T.tensor(np.array(past_window["ask_low"][-(self.prediction_depth+self.lookback_depth):-(self.prediction_depth)]) - np.array(past_window["ask_low"][-(self.prediction_depth+self.lookback_depth+1):-(self.prediction_depth+1)]))

        x = T.stack([x_bid_high, x_bid_low, 
                     x_ask_high, x_ask_low,
                     ATR, BB_HIGH, BB_LOW,
                     DC_HIGH, DC_LOW, KEL_HIGH,
                     KEL_LOW, ULCER, SMA,
                     ADX, ADX_NEG, ADX_POS,
                     AROON, AROON_UP, AROON_DOWN,
                     CCI, DPO, EMA,
                     ICHIMOKU_BASE, ICHIMOKU_CONV,
                     ICHIMOKU_A, ICHIMOKU_B,
                     KST, KST_SIG, MACD,
                     MACD_SIG, MI, PSAR,
                     STC, TRIX, VI_NEG, VI_POS]).to(T.float32)

        y_bid_high = T.tensor(np.array(past_window["bid_high"][-(self.prediction_depth):]) - np.array(past_window["bid_high"][-(self.prediction_depth+1):-1]))
        y_bid_low = T.tensor(np.array(past_window["bid_low"][-(self.prediction_depth):]) - np.array(past_window["bid_low"][-(self.prediction_depth+1):-1]))
        y_ask_high = T.tensor(np.array(past_window["ask_high"][-(self.prediction_depth):]) - np.array(past_window["ask_high"][-(self.prediction_depth+1):-1]))
        y_ask_low = T.tensor(np.array(past_window["ask_low"][-(self.prediction_depth):]) - np.array(past_window["ask_low"][-(self.prediction_depth+1):-1]))
        y = T.stack([(y_bid_high + y_ask_high) / 2, (y_bid_low + y_ask_low) / 2]).to(T.float32)


        self.training_examples.append({"x": x, "y": y})

        # self.train()

        return None


# ------------------------------------------------------------------------------

class Environment(ABC):
    pass

class Simulation(Environment):

    def __init__(self, agent, broker):
        
        # leverage=20, unit=0.1, units_per_contract=100, min_units=1.0, max_units=100

        self.agent = agent
        self.broker = broker


    def step(self, past_window, global_offset):

        self.agent.capital = self.agent.init_capital + sum([t.total_gain for t in self.broker.finished_trades])

        current_state = deepcopy(past_window.iloc[-1])
        
        if self.agent.ma_loss.mean() > self.agent.loss_thr:
            self.agent.collect_trade(past_window, current_state, global_offset)
        else:
            trade = self.agent.trade(past_window, current_state, global_offset)
            if trade != None:
                self.broker.create_trade(trade)
            self.broker.check_trades(current_bid_high = current_state["bid_high"], current_bid_low = current_state["bid_low"],
                                    current_ask_high = current_state["ask_high"], current_ask_low = current_state["ask_low"],
                                    current_timestamp = current_state["timestamp"])
        self.agent.train()

        return None

    def backtest(self, data, lookback_window):
        
        for i in range(lookback_window, len(data)):
            
            self.step(data.iloc[i-lookback_window:i], i - lookback_window)

            print("\n")
            print("simulation:")
            print("\tindex:", i - lookback_window)
            print("\tgain: ", self.agent.capital)
            print("model ma_loss:", np.mean(self.agent.ma_loss))
            print("\n\n\n\n")

            if self.agent.capital < 0:
                break

# ------------------------------------------------------------------------------

if __name__ == '__main__':

    T.random.manual_seed(0)
    np.random.seed(0)

    past_data = load_price()
    past_data = past_data.sort_values("timestamp")
    # past_data = past_data[-24*100:]
    past_data["timestamp"] = range(len(past_data))
    
    agent = DLAgent(init_capital = 1000, lookback_depth = 24, prediction_depth = 10, loss_threshold = 10, lr = 1e-3, 
                    max_number_of_training_examples = 50, hidden_size = 512, loss_moving_average = 50)
    broker = SimulationBroker(max_number_of_parallel_trades_opened = 100)
    simulation = Simulation(agent = agent, broker = broker)

    simulation.backtest(past_data, lookback_window = 24 * 22)
    

    # EVAL & PLOT

    print("\nnumber of completed trades:", len(simulation.broker.finished_trades))
    print("\nnumber of open trades:", len(simulation.broker.valid_non_finished_trades))
    print("\nnumber of invalid trades:", len(simulation.broker.invalid_trades)) 

    print("\nsum total gain:", sum([t.total_gain for t in simulation.broker.finished_trades]))
    print("\nwin rate:", sum([t.total_gain > 0 for t in simulation.broker.finished_trades]) / len(simulation.broker.finished_trades))


    fig = plt.figure(figsize=(10, 7))
    ax = plt.subplot()

    up = past_data[past_data["bid_close"]>=past_data["bid_open"]]
    down = past_data[past_data["bid_close"]<past_data["bid_open"]]
    
    ax.bar(up["timestamp"], up["bid_high"] - up["bid_close"], 0.2, bottom = up["bid_close"], color = "black", alpha=0.4)
    ax.bar(up["timestamp"], up["bid_low"] - up["bid_open"],   0.2, bottom = up["bid_open"],  color = "black", alpha=0.4)
    ax.bar(up["timestamp"], up["bid_close"] - up["bid_open"], 1,   bottom = up["bid_open"],  color = "green", alpha=0.4)

    ax.bar(down["timestamp"], down["bid_high"] - down["bid_close"], 0.2, bottom = down["bid_close"], color = "black", alpha=0.4)
    ax.bar(down["timestamp"], down["bid_low"] - down["bid_open"],   0.2, bottom = down["bid_open"],  color = "black", alpha=0.4)
    ax.bar(down["timestamp"], down["bid_close"] - down["bid_open"], 1,   bottom = down["bid_open"],  color = "red",   alpha=0.4)

    for trade in simulation.broker.finished_trades:
        if trade.direction == "buy":
            if trade.total_gain > 0:
                ax.plot([trade.datetime_issued, trade.datetime_opened], [trade.open_price, trade.open_price], 0.3, color = "lime")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.stop_loss], 0.3, color = "lime")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.take_profit], 0.3, color = "lime")
            else:
                ax.plot([trade.datetime_issued, trade.datetime_opened], [trade.open_price, trade.open_price], 0.3, color = "red")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.stop_loss], 0.3, color = "red")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.take_profit], 0.3, color = "red")
        else:
            if trade.total_gain > 0:
                ax.plot([trade.datetime_issued, trade.datetime_opened], [trade.open_price, trade.open_price], 0.3, color = "green")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.stop_loss], 0.3, color = "green")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.take_profit], 0.3, color = "green")
            else:
                ax.plot([trade.datetime_issued, trade.datetime_opened], [trade.open_price, trade.open_price], 0.3, color = "brown")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.stop_loss], 0.3, color = "brown")
                ax.plot([trade.datetime_opened, trade.datetime_closed], [trade.open_price, trade.take_profit], 0.3, color = "brown")


    plt.show()
    