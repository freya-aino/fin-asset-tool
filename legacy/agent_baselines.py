from pitchfork import get_pitchforks
from abc import ABC,  abstractmethod
import numpy as np
from copy import deepcopy


class Agent(ABC):

    @abstractmethod
    def __init__(self, broker, environment):
        pass

    @abstractmethod
    def trade(self, past_window, state):
        # decides whether to open a trade
        pass

    # def get_pitchforks_hl(self, past_window_high, past_window_low, past_window_close):
    #     return get_pitchforks(past_window_high, past_window_low, past_window_close, self.env.lookback_depth)

    # def get_pitchfork_hl(self, past_data_high, past_data_low, past_data_close, last=1):
    #     offset = len(past_data_high) - self.env.window_size
    #     last_pivots = self.get_pitchforks_hl(past_data_high[-self.env.window_size:], past_data_low[-self.env.window_size:], past_data_close[-self.env.window_size:])
    #     last_pivots = [(p[0] + offset, p[1]) for p in last_pivots[-last]]
    #     return last_pivots

    # def get_pitchforks(self, past_window, loock_back_depth):
    #     return get_pitchforks(past_window, loock_back_depth)

    # def get_pitchfork(self, past_data, last=1):
    #     offset = len(past_data) - self.env.window_size
    #     last_pivots = self.get_pitchforks(past_data[-self.env.window_size:])
    #     # print("# pitchforks", len(last_pivots))
    #     last_pivots = [(p[0] + offset, p[1]) for p in last_pivots[-last]]
    #     return last_pivots

    # def trade_open(self):
    #     return self.env.trade_open()

    def compute_pitchfork(self, past_window):
        pitchfork_bid = Pitchfork(self.get_pitchfork_hl(past_window['bid_high'], past_window['bid_low'], past_window['bid_close'], last=1), self.env.reach_level)
        pitchfork_ask = Pitchfork(self.get_pitchfork_hl(past_window['ask_high'], past_window['ask_low'], past_window['ask_close'], last=1), self.env.reach_level)

        if pitchfork_ask.pivots in self.seen_pitchforks_ask.keys():
            pitchfork = self.seen_pitchforks_ask[pitchfork_ask.pivots]
        else:
            self.seen_pitchforks_ask[pitchfork_ask.pivots] = pitchfork_ask
        self.current_pitchfork_ask = pitchfork_ask

        if pitchfork_bid.pivots in self.seen_pitchforks_bid.keys():
            pitchfork = self.seen_pitchforks_bid[pitchfork_bid.pivots]
        else:
            self.seen_pitchforks_bid[pitchfork_bid.pivots] = pitchfork_bid
        self.current_pitchfork_bid = pitchfork_bid


class MgtAgent(Agent):

    def __init__(self, broker, environment):
        self.broker = broker
        self.env = environment
        self.seen_pitchforks = {}

    def trade(self, past_window, tick_state=None):
        state = deepcopy(past_window.iloc[-1].to_dict())
        if tick_state:
            state = tick_state
        state['step'] = self.env.step_i

        i = self.env.step_i

        for trade in self.broker.opened_positions:
            if trade['state_open']['pitchfork'].breakout_support(i, past_window) or trade['state_open']['pitchfork'].breakout_resistance(i, past_window):
                self.broker.close_trades(state)

        pitchfork = self.current_pitchfork
        trade_open = self.trade_open()


        # Ask price at which a seller is willing to sell
        if not trade_open and pitchfork.up() and pitchfork.exiting_support(i, past_window, tick_state): # pitchfork.price_is_in_support_reach(i, state['ask_low']):
            quantity_buy = self.env.quantity_buy()
            # stop_loss = pitchfork.support_at(i) - pitchfork.reach
            stop_loss = pitchfork.support_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) - pitchfork.reach
            price_to_buy = pitchfork.support_at(i) + pitchfork.reach
            if tick_state:
                price_to_buy = state['ask_high']  # pitchfork.support_at(i) + pitchfork.reach
            take_profit = pitchfork.median_at(i)  # + pitchfork.reach
            state['pitchfork'] = pitchfork
            stop_loss = price_to_buy - 2 * pitchfork.sl
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

        # Bid price at which a buyer is willing to buy
        if not trade_open and pitchfork.down() and pitchfork.exiting_resistance(i, past_window, tick_state):  # and pitchfork.price_is_in_resistance_reach(i, state['bid_high']):
            quantity_sell = self.env.quantity_sell()
            # stop_loss = pitchfork.resistance_at(i) + pitchfork.reach
            # stop_loss = pitchfork.resistance_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) + pitchfork.reach
            price_to_sell = pitchfork.resistance_at(i) - pitchfork.reach
            if tick_state:
                price_to_sell = state['bid_low']  # pitchfork.resistance_at(i) - pitchfork.reach
            take_profit = pitchfork.median_at(i)  # - pitchfork.reach
            stop_loss = price_to_sell + 2 * pitchfork.sl
            state['pitchfork'] = pitchfork
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        if not trade_open and pitchfork.up() and pitchfork.breakout_support(i, past_window):
            # Close trade before opening the breakout one
            #self.env.broker.close_trades(state)
            quantity_sell = self.env.quantity_sell()
            # TODO : trailing take profit or stop loss
            # stop_loss = pitchfork.support_at(i)
            # stop_loss = pitchfork.support_at(i - abs(pitchfork.low[0] - pitchfork.median_point[0]))
            price_to_sell = pitchfork.support_at(i) - pitchfork.reach
            if tick_state:
                price_to_sell = state['bid_low']
            stop_loss = price_to_sell + pitchfork.sl
            take_profit = price_to_sell - (pitchfork.median_at(i) - pitchfork.support_at(i))
            state['pitchfork'] = pitchfork
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        if not trade_open and pitchfork.down() and pitchfork.breakout_resistance(i, past_window):
            # Close trades before opening a breakout
            #self.broker.close_trades(state)
            quantity_buy = self.env.quantity_buy()
            # TODO : trailing take profit or stop loss
            # stop_loss = pitchfork.resistance_at(i)
            # stop_loss = pitchfork.resistance_at(i - abs(pitchfork.low[0] - pitchfork.median_point[0]))
            price_to_buy = pitchfork.resistance_at(i) + pitchfork.reach
            if tick_state:
                price_to_buy = state['ask_high']
            stop_loss = price_to_buy - pitchfork.sl
            take_profit = price_to_buy + (pitchfork.resistance_at(i) - pitchfork.median_at(i))
            state['pitchfork'] = pitchfork
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

class ZenonAgent(Agent):

    def __init__(self, broker, environment):
        self.broker = broker
        self.env = environment
        self.seen_pitchforks_bid = {}
        self.seen_pitchforks_ask = {}
        self.current_pitchfork_ask = None
        self.current_pitchfork_bid = None

    def trade(self, past_window, tick_state=None):

        self.spread = abs((past_window['bid_high'] - past_window['ask_high']).mean())

        state = deepcopy(past_window.iloc[-1].to_dict())
        if tick_state:
            state = deepcopy(tick_state)

        state['step'] = self.env.step_i

        i = self.env.step_i
        pitchfork_bid = self.current_pitchfork_bid
        pitchfork_ask = self.current_pitchfork_ask

        # trade_open = self.trade_open()

        open_buy = len([pos for pos in self.env.simulation_broker.opened_positions if pos['direction'] == 'buy']) > 0
        open_sell = len([pos for pos in self.env.simulation_broker.opened_positions if pos['direction'] == 'sell']) > 0

        # Ask price at which a seller is willing to sell
        if not open_buy and pitchfork_ask.up() and pitchfork_ask.exiting_support(i, past_window, tick_state): # pitchfork.price_is_in_support_reach(i, state['ask_low']):
            # Zenon d'élée
            quantity_buy = self.env.quantity_buy()
            if not open_sell:
                quantity_buy = max([int(quantity_buy/2.0), 1])
            # stop_loss = pitchfork.support_at(i) - pitchfork.reach
            # stop_loss = pitchfork.support_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) - pitchfork.reach
            price_to_buy = pitchfork_ask.support_at(i) + pitchfork_ask.reach
            stop_loss = price_to_buy - 2 * pitchfork_ask.sl - self.spread
            if tick_state:
                price_to_buy = state['ask_high']  # pitchfork.support_at(i) + pitchfork.reach
            take_profit = pitchfork_ask.median_at(i)  # + pitchfork.reach
            state['pitchfork'] = pitchfork_ask
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

        # Bid price at which a buyer is willing to buy
        if not open_sell and pitchfork_bid.down() and pitchfork_bid.exiting_resistance(i, past_window, tick_state):  # and pitchfork.price_is_in_resistance_reach(i, state['bid_high']):
            quantity_sell = self.env.quantity_sell()
            if not open_buy:
                quantity_sell = max([int(quantity_sell/2.0), 1])
            # stop_loss = pitchfork.resistance_at(i) + pitchfork.reach
            # stop_loss = pitchfork.resistance_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) + pitchfork.reach
            price_to_sell = pitchfork_bid.resistance_at(i) - pitchfork_bid.reach
            stop_loss = price_to_sell + 2 * pitchfork_bid.sl + self.spread
            if tick_state:
                price_to_sell = state['bid_low']  # pitchfork.resistance_at(i) - pitchfork.reach
            take_profit = pitchfork_bid.median_at(i)  # - pitchfork.reach
            state['pitchfork'] = pitchfork_bid
            # print(state)
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        # Bid price at which a buyer is willing to buy
        if False and not open_sell and pitchfork_bid.up() and pitchfork_bid.breakout_support(i, past_window, tick_state):
            quantity_sell = self.env.quantity_sell()
            if not open_buy:
                quantity_sell = max([int(quantity_sell/2.0), 1])
            # TODO : trailing take profit or stop loss
            # stop_loss = pitchfork.support_at(i)
            # stop_loss = pitchfork.support_at(i - abs(pitchfork.low[0] - pitchfork.median_point[0]))
            price_to_sell = pitchfork_bid.support_at(i) - pitchfork_bid.reach
            stop_loss = price_to_sell + 2 * pitchfork_bid.sl
            if tick_state:
                price_to_sell = state['bid_low']
            take_profit = price_to_sell - (pitchfork_bid.median_at(i) - pitchfork_bid.support_at(i))
            state['pitchfork'] = pitchfork_bid
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        # Ask price at which a seller is willing to sell
        if False and not open_buy and pitchfork_ask.down() and pitchfork_ask.breakout_resistance(i, past_window, tick_state):
            quantity_buy = self.env.quantity_buy()
            if not open_sell:
                quantity_buy = max([int(quantity_buy/2.0), 1])
            # TODO : trailing take profit or stop loss
            # stop_loss = pitchfork.resistance_at(i)
            # stop_loss = pitchfork.resistance_at(i - abs(pitchfork.low[0] - pitchfork.median_point[0]))
            price_to_buy = pitchfork_ask.resistance_at(i) + pitchfork_ask.reach
            stop_loss = price_to_buy - 2 * pitchfork_ask.sl
            if tick_state:
                price_to_buy = state['ask_high']
            take_profit = price_to_buy + (pitchfork_ask.resistance_at(i) - pitchfork_ask.median_at(i))
            state['pitchfork'] = pitchfork_ask
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

class BaselineAgent(Agent):

    def __init__(self, broker, environment):
        self.broker = broker
        self.env = environment
        self.seen_pitchforks = {}

    def trade(self, past_window, tick_state=None):

        state = deepcopy(past_window.iloc[-1].to_dict())
        if tick_state:
            state = tick_state
        state['step'] = self.env.step_i

        i = self.env.step_i
        pitchfork = self.current_pitchfork
        trade_open = self.trade_open()

        # Ask price at which a seller is willing to sell
        if not trade_open and pitchfork.up() and pitchfork.exiting_support(i, past_window, tick_state): # pitchfork.price_is_in_support_reach(i, state['ask_low']):
            quantity_buy = self.env.quantity_buy()
            # stop_loss = pitchfork.support_at(i) - pitchfork.reach
            stop_loss = pitchfork.support_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) - pitchfork.reach
            price_to_buy = pitchfork.support_at(i) + pitchfork.reach
            if tick_state:
                price_to_buy = state['ask_high']  # pitchfork.support_at(i) + pitchfork.reach
            take_profit = pitchfork.median_at(i)  # + pitchfork.reach
            state['pitchfork'] = pitchfork
            stop_loss = price_to_buy - 2 * pitchfork.sl
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

        # Bid price at which a buyer is willing to buy
        if not trade_open and pitchfork.down() and pitchfork.exiting_resistance(i, past_window, tick_state):  # and pitchfork.price_is_in_resistance_reach(i, state['bid_high']):
            quantity_sell = self.env.quantity_sell()
            # stop_loss = pitchfork.resistance_at(i) + pitchfork.reach
            # stop_loss = pitchfork.resistance_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) + pitchfork.reach
            price_to_sell = pitchfork.resistance_at(i) - pitchfork.reach
            if tick_state:
                price_to_sell = state['bid_low']  # pitchfork.resistance_at(i) - pitchfork.reach
            take_profit = pitchfork.median_at(i)  # - pitchfork.reach
            stop_loss = price_to_sell + 2 * pitchfork.sl
            state['pitchfork'] = pitchfork
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        if not trade_open and pitchfork.up() and pitchfork.breakout_support(i, past_window):
            quantity_sell = self.env.quantity_sell()
            # TODO : trailing take profit or stop loss
            # stop_loss = pitchfork.support_at(i)
            # stop_loss = pitchfork.support_at(i - abs(pitchfork.low[0] - pitchfork.median_point[0]))
            price_to_sell = pitchfork.support_at(i) - pitchfork.reach
            if tick_state:
                price_to_sell = state['bid_low']
            stop_loss = price_to_sell + pitchfork.sl
            take_profit = price_to_sell - (pitchfork.median_at(i) - pitchfork.support_at(i))
            state['pitchfork'] = pitchfork
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        if not trade_open and pitchfork.down() and pitchfork.breakout_resistance(i, past_window):
            quantity_buy = self.env.quantity_buy()
            # TODO : trailing take profit or stop loss
            # stop_loss = pitchfork.resistance_at(i)
            # stop_loss = pitchfork.resistance_at(i - abs(pitchfork.low[0] - pitchfork.median_point[0]))
            price_to_buy = pitchfork.resistance_at(i) + pitchfork.reach
            if tick_state:
                price_to_buy = state['ask_high']
            stop_loss = price_to_buy - pitchfork.sl
            take_profit = price_to_buy + (pitchfork.resistance_at(i) - pitchfork.median_at(i))
            state['pitchfork'] = pitchfork
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

class BreakoutAgent(Agent):

    def __init__(self, broker, environment):
        self.broker = broker
        self.env = environment
        self.seen_pitchforks = {}

    def trade(self, past_window, tick_state=None):

        state = deepcopy(past_window.iloc[-1].to_dict())
        if tick_state:
            state = tick_state
        state['step'] = self.env.step_i

        quantity_buy = self.env.quantity_buy()
        quantity_sell = self.env.quantity_sell()

        i = self.env.step_i
        pitchfork = self.current_pitchfork

        if quantity_sell > 0 and not self.trade_open() and pitchfork.up() and pitchfork.breakout_support(i, past_window):
            # TODO : trailing take profit or stop loss
            stop_loss = pitchfork.support_at(i)
            # price_to_sell = state['bid_low']  #  pitchfork.support_at(i) - pitchfork.reach
            price_to_sell = pitchfork.support_at(i) - pitchfork.reach
            if tick_state:
                price_to_sell = state['bid_low']
            take_profit = price_to_sell - (pitchfork.median_at(i) - pitchfork.support_at(i))
            state['pitchfork'] = pitchfork
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

        if quantity_buy > 0 and not self.trade_open() and pitchfork.down() and pitchfork.breakout_resistance(i, past_window):
            # TODO : trailing take profit or stop loss
            stop_loss = pitchfork.resistance_at(i)
            # price_to_buy = state['ask_high']  #  pitchfork.resistance_at(i) + pitchfork.reach
            price_to_buy = pitchfork.resistance_at(i) + pitchfork.reach
            if tick_state:
                price_to_buy = state['ask_high']
            take_profit = price_to_buy + (pitchfork.resistance_at(i) - pitchfork.median_at(i))
            state['pitchfork'] = pitchfork
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

class InnerAgent(Agent):

    def __init__(self, broker, environment):
        self.broker = broker
        self.env = environment
        self.seen_pitchforks = {}

    def trade(self, past_window, tick_state=None):

        state = deepcopy(past_window.iloc[-1].to_dict())
        if tick_state:
            state = tick_state
        state['step'] = self.env.step_i

        quantity_buy = self.env.quantity_buy()
        quantity_sell = self.env.quantity_sell()

        i = self.env.step_i
        pitchfork = self.current_pitchfork

        # Ask price at which a seller is willing to sell
        if quantity_buy > 0 and not self.trade_open() and pitchfork.up() and pitchfork.exiting_support(i, past_window, tick_state): # pitchfork.price_is_in_support_reach(i, state['ask_low']):
            stop_loss = pitchfork.support_at(i) - pitchfork.reach
            # stop_loss = pitchfork.support_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) - pitchfork.reach
            price_to_buy = pitchfork.support_at(i) + pitchfork.reach
            if tick_state:
                price_to_buy = state['ask_high']  # pitchfork.support_at(i) + pitchfork.reach
            take_profit = pitchfork.median_at(i)  # + pitchfork.reach
            state['pitchfork'] = pitchfork
            self.broker.open_buy(state, stop_loss, take_profit, price_to_buy, quantity_buy)

        # Bid price at which a buyer is willing to buy
        if quantity_sell > 0 and not self.trade_open() and pitchfork.down() and pitchfork.exiting_resistance(i, past_window, tick_state):  # and pitchfork.price_is_in_resistance_reach(i, state['bid_high']):
            stop_loss = pitchfork.resistance_at(i) + pitchfork.reach
            # stop_loss = pitchfork.resistance_at(i + abs(pitchfork.low[0] - pitchfork.median_point[0])) + pitchfork.reach
            price_to_sell = pitchfork.resistance_at(i) - pitchfork.reach
            if tick_state:
                price_to_sell = state['bid_low']  # pitchfork.resistance_at(i) - pitchfork.reach
            take_profit = pitchfork.median_at(i)  # - pitchfork.reach
            state['pitchfork'] = pitchfork
            self.broker.open_sell(state, stop_loss, take_profit, price_to_sell, quantity_sell)

class RandomAgent(Agent):

    def __init__(self, broker, environment):
        self.broker = broker
        self.env = environment

    def trade(self, past_window, state):

        if not self.trade_open() and np.random.random() < 0.1:
            if np.random.random() > 0.5:
                stop_loss = state['bid_low'] + 10
                take_profit = state['bid_low'] - 10
                self.broker.open_sell(state, stop_loss, take_profit, state['bid_low'])
            else:
                stop_loss = state['ask_high'] - 10
                take_profit = state['ask_high'] + 10
                self.broker.open_buy(state, stop_loss, take_profit, state['ask_high'])
            #print(self.broker.opened_positions[-1])



class Pitchfork:

    def __init__(self, pivots, reach_level=0.25):
        
        self.pivots = pivots
        self.reach_level = reach_level

        x1, x2, x3 = [self.pivots[j][0] for j in range(3)]
        y1, y2, y3 = [self.pivots[j][1] for j in range(3)]

        xm, ym = (x2 + x3) / 2.0, (y2 + y3) / 2.0

        self.median_point = (xm, ym)

        self.slope = (ym - y1) / (xm - x1)

        self.median_intercept = y1 - self.slope * x1

        if y2 < y3:
            low = x2, y2
            high = x3, y3
        else:
            low = x3, y3
            high = x2, y2

        self.low = low
        self.high = high
        self.distance_to_median = ((self.high[0]-self.low[0])**2 + (self.high[1]-self.low[1])**2)**0.5 / 2.0

        self.sl = (self.high[1] - self.low[1]) * self.reach_level / 2.0

        self.resistance_intercept = high[1] - self.slope * high[0]
        self.support_intercept = low[1] - self.slope * low[0]
        self.reach = (self.median_at(1) - self.support_at(1)) * self.reach_level

        self.last_support_exit = 0
        self.last_resistance_exit = 0

        lx, ly = ((x2 + x3) / 2) - x1, ((y2 + y3) / 2) - y1
        bx, by = 1., 0.
        self.steepness = np.rad2deg(np.arccos(np.inner([lx, ly], [bx, by]) / (np.sum(np.abs([lx, ly])) * np.sum(np.abs([bx, by])))))

        self.weight = (90 - self.steepness)/90.0

        self.broke_out = False

        self.broke_out_opened = False

    def exiting_support(self, i, past_window, tick_state=None):
        entered = False
        exiting = False
        last_exit = self.last_support_exit
        start = last_exit
        if last_exit == 0:
            start = int(self.low[0])  # int(self.median_point[0])

        # if not tick_state:
        #     i = i - 1

        # we consider the last state in case it comes from streaming
        prices_low = np.array(past_window['ask_low'])
        prices_high = np.array(past_window['ask_high'])
        # if tick_state:
        #     prices_low = np.append(prices_low, tick_state['ask_low'])
        #     prices_high = np.append(prices_high, tick_state['ask_high'])

        for j in range(start, len(prices_low)):
            if entered:
                support_p = self.support_at(j)
                if prices_low[j] - (support_p + self.reach) > 0:
                    # Is above the support reach line
                    exiting = True
                    entered = False
                    last_exit = j
                    # print("exit : ", j, prices_low[j])
            if self.price_is_in_support_reach(j, prices_high[j]) and not entered:
                entered = True
                exiting = False
        self.last_support_exit = last_exit
        if i == last_exit + 1 and exiting:
            # print("Support exit :", i, last_exit, tick_state)
            # time.sleep(1)
            return True
        return False

    def exiting_resistance(self, i, past_window, tick_state=None):
        entered = False
        exiting = False
        last_exit = self.last_resistance_exit
        start = last_exit
        if last_exit == 0:
            start = int(self.high[0])  # int(self.median_point[0])

        # we consider the last state in case it comes from streaming
        # if not tick_state:
        #     i = i - 1

        # we consider the last state in case it comes from streaming
        prices_low = np.array(past_window['bid_low'])
        prices_high = np.array(past_window['bid_high'])

        # if tick_state:
        #     prices_low = np.append(prices_low, tick_state['bid_low'])
        #     prices_high = np.append(prices_high, tick_state['bid_high'])

        for j in range(start, len(prices_low)):
            if entered:
                if (self.resistance_at(j) - self.reach) - prices_high[j] > 0:
                    # Is below the resistance reach line
                    exiting = True
                    entered = False
                    last_exit = j
            if self.price_is_in_resistance_reach(j, prices_low[j]) and not entered:
                entered = True
                exiting = False
        self.last_resistance_exit = last_exit
        if i == last_exit + 1 and exiting:
            # print("resistance exit :", i)
            # time.sleep(1)
            return True
        return False

    def breakout_support(self, i, past_window, tick_state=None):
        entered = False
        breakout = False
        last_exit = 0
        start = int(self.median_point[0])
        # if not tick_state:
        #     i = i - 1

        for j in range(start, len(past_window)):
            price = past_window.iloc[j]
            if entered:
                if (self.support_at(j) - self.reach) - price['ask_low'] > 0:
                    # Is below the support outer reach
                    breakout = True
                    entered = False
                    last_exit = j
            if self.price_is_in_support_reach(j, price['ask_low']) and not entered:
                entered = True
                breakout = False

        if not self.broke_out_opened and i == last_exit + 1 and breakout and self.broke_out:
            print("breakout", i)
            self.broke_out_opened = True
            return True
        if i == last_exit + 1 and breakout and not self.broke_out:
            self.broke_out = True
        return False

    def breakout_resistance(self, i, past_window, tick_state=None):
        entered = False
        breakout = False
        last_exit = 0
        start = int(self.median_point[0])
        # if not tick_state:
        #     i = i - 1

        for j in range(start, len(past_window)):
            price = past_window.iloc[j]
            if entered:
                if price['bid_high'] - (self.resistance_at(j) + self.reach) > 0:
                    # Is above the resistance outer reach line
                    # print("breakout ", j, i)
                    breakout = True
                    entered = False
                    last_exit = j
            if self.price_is_in_resistance_reach(j, price['bid_high']) and not entered:
                entered = True
                breakout = False

        if not self.broke_out_opened and i == last_exit + 1 and breakout and self.broke_out:
            print("breakout", i)
            self.broke_out_opened = True
            return True
        if i == last_exit + 1 and breakout and not self.broke_out:
            self.broke_out = True
        return False

    def up(self):
        return self.pivots[0][1] < self.median_point[1]

    def down(self):
        return self.pivots[0][1] > self.median_point[1]

    def resistance_at(self, i):
        return self.slope * i + self.resistance_intercept

    def support_at(self, i):
        return self.slope * i + self.support_intercept

    def median_at(self, i):
        return self.slope * i + self.median_intercept

    #def reach(self, i):
    #    return (self.median_at(i) - self.support_at(i))*self.reach_level

    def price_is_in_support_reach(self, i, p):
        support_p = self.support_at(i)
        if p - support_p > 0:
            # Is above the support  line
            if p - (support_p + self.reach) < 0:
                # Is below the support inner reach line
                return True
        return False

    def price_is_in_resistance_reach(self, i, p):
        resistance_p = self.resistance_at(i)
        if resistance_p - p > 0:
            # Is below the resistance line
            if p - (resistance_p - self.reach) > 0:
                # Is above the resistance reach line
                return True
        return False

    def price_is_in_median_reach(self, i, p):
        return False

    def plot(self, ax):
        x1, x2, x3 = [self.pivots[j][0] for j in range(3)]
        y1, y2, y3 = [self.pivots[j][1] for j in range(3)]
        colors = ["Red", "Green", "Yellow", "Purple", "Pink", "Orange"]
        alpha = 0.7
        alpha_ = 0.5
        if x1 != 0 and x2 != 0.0:
            xm, ym = (x2 + x3) / 2, (y2 + y3) / 2
            slope = (ym - y1) / (xm - x1)
            intercept = y1 - slope * x1
            xm_lim = xm + (xm - x1)
            ym_lim = slope * (xm_lim) + intercept
            # median
            ax.plot((x1, xm + 100), (y1, self.median_at(xm + 100)), linewidth=2, c=colors[0], alpha=alpha)
            # resistance
            ax.plot((self.high[0], self.high[0] + 100), (self.high[1], self.resistance_at(self.high[0] + 100)), linewidth=3, c=colors[3], alpha=alpha)
            ax.plot((self.high[0], self.high[0] + 100), (self.high[1] + self.reach, self.resistance_at(self.high[0] + 100) + self.reach),
                    linewidth=3, c=colors[3], alpha=0.5)
            ax.plot((self.high[0], self.high[0] + 100), (self.high[1] - self.reach, self.resistance_at(self.high[0] + 100) - self.reach),
                    linewidth=3, c=colors[3], alpha=0.5)
            # support
            ax.plot((self.low[0], self.low[0]+100), (self.low[1], self.support_at(self.low[0]+100)), linewidth=3, c=colors[4], alpha=alpha)
            ax.plot((self.low[0], self.low[0]+100), (self.low[1] + self.reach, self.support_at(self.low[0]+100) + self.reach), linewidth=3, c=colors[4], alpha=alpha_)
            ax.plot((self.low[0], self.low[0]+100), (self.low[1] - self.reach, self.support_at(self.low[0]+100) - self.reach), linewidth=3, c=colors[4], alpha=alpha_)
            ax.plot((x2, x3), (y2, y3), linewidth=3, c=colors[1], alpha=alpha)

