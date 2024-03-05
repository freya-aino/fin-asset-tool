from copy import deepcopy
import numpy as np
from numpy.lib.function_base import median
import ta
# from history import load_price

import matplotlib.pyplot as plt

def high_pivots(seq, global_index):
    l = int(np.floor(len(seq)/2))
    if np.argmax(seq) == l:
        return {"i": global_index - l, "p": np.max(seq)}
    else:
        return {"i": np.nan, "p": np.nan}

def low_pivots(seq, global_index):
    l = int(np.floor(len(seq)/2))
    if np.argmin(seq) == l:
        return {"i": global_index - l, "p": np.min(seq)}
    else:
        return {"i": np.nan, "p": np.nan}

def valuewhenchange(sequential_past, occ):
    if len(sequential_past) > 1:
        valid_past = sequential_past[1:][sequential_past[1:] != sequential_past[:-1]]
        if len(valid_past) > occ:
            return valid_past[-occ-1]
    return sequential_past[-1]

def calc_dev(base_price, price):
    return 100 * (price - base_price) / price

def any_line_is_nan(line):
    return np.any(np.isnan([line[0]["i"], line[0]["p"], line[1]["i"], line[1]["p"]]))

def pivot_found_high(pivot, last_pivot, dev, dev_threshold, last_line, last_pivot_direction):

    last_line = deepcopy(last_line)

    if not any_line_is_nan(last_line) and last_pivot_direction == "high":
        if pivot["p"] > last_pivot["p"]:
            last_line[1] = {"i": pivot["i"], "p": pivot["p"]}
            return last_line, last_line
        else:
            return last_line, [{"i": np.nan, "p": np.nan}, {"i": np.nan, "p": np.nan}]
    else:
        if abs(dev) > dev_threshold:
            return last_line, [{"i": last_pivot["i"], "p": last_pivot["p"]}, {"i": pivot["i"], "p": pivot["p"]}]
        else:
            return last_line, [{"i": np.nan, "p": np.nan}, {"i": np.nan, "p": np.nan}]

def pivot_found_low(pivot, last_pivot, dev, dev_threshold, last_line, last_pivot_direction):

    last_line = deepcopy(last_line)

    if not any_line_is_nan(last_line) and last_pivot_direction == "low":

        if pivot["p"] < last_pivot["p"]:
            last_line[1] = {"i": pivot["i"], "p": pivot["p"]}
            return last_line, last_line
        else:
            return last_line, [{"i": np.nan, "p": np.nan}, {"i": np.nan, "p": np.nan}]
    else:
        if abs(dev) > dev_threshold:
            return last_line, [{"i": last_pivot["i"], "p": last_pivot["p"]}, {"i": pivot["i"], "p": pivot["p"]}]
        else:
            return last_line, [{"i": np.nan, "p": np.nan}, {"i": np.nan, "p": np.nan}]

def find_pivots(H_pivot, L_pivot, last_pivot, second_last_pivot, 
                last_line, last_pivot_direction, dev_threshold,
                second_last_pivot_ref_buffer, last_pivot_ref_buffer):

    if not np.isnan(H_pivot["i"]):

        dev = calc_dev(last_pivot["p"], H_pivot["p"])
        last_line, line = pivot_found_high(H_pivot, last_pivot, dev, dev_threshold, last_line, last_pivot_direction)

        if not any_line_is_nan(line):
            if line != last_line:
                second_last_pivot_ref_buffer = np.append(second_last_pivot_ref_buffer, last_line[0])
                last_pivot_ref_buffer = np.append(last_pivot_ref_buffer, last_line[1])

            last_line = deepcopy(line)
            last_pivot_direction = "high"
            second_last_pivot = last_pivot
            last_pivot = H_pivot

    elif not np.isnan(L_pivot["i"]):

        dev = calc_dev(last_pivot["p"], L_pivot["p"])
        last_line, line = pivot_found_low(L_pivot, last_pivot,  dev, dev_threshold, last_line, last_pivot_direction)

        if not any_line_is_nan(line):
            if line != last_line:
                second_last_pivot_ref_buffer = np.append(second_last_pivot_ref_buffer, last_line[0])
                last_pivot_ref_buffer = np.append(last_pivot_ref_buffer, last_line[1])

            last_line = deepcopy(line)
            last_pivot_direction = "low"
            second_last_pivot = last_pivot
            last_pivot = L_pivot

    # else:
    #     print("neither high pivots nor low pivots could be found")

    third_last_pivot_ = valuewhenchange(second_last_pivot_ref_buffer, 1)
    second_last_pivot_ = valuewhenchange(second_last_pivot_ref_buffer, 0)
    last_pivot_ = valuewhenchange(last_pivot_ref_buffer, 0)

    return [third_last_pivot_, second_last_pivot_, last_pivot_], [last_pivot, second_last_pivot, last_line, last_pivot_direction, second_last_pivot_ref_buffer, last_pivot_ref_buffer]

def get_pitchforks(price, lookback_depth, last_pivot_direction = "high"):

    high_seq = np.array(price["bid_high"])
    low_seq = np.array(price["bid_low"])

    atr = ta.volatility.AverageTrueRange(price['bid_high'], price['bid_low'], price['bid_close'], 10).average_true_range()
    dev_threshold = np.array(atr / price['bid_close'] * 100 * 3)

    last_pivot = {"i": 0., "p": 0.}
    second_last_pivot = {"i": 0., "p": 0.}
    last_line = [{"i": np.nan, "p": np.nan}, {"i": np.nan, "p": np.nan}]
    second_last_pivot_ref_buffer = np.array([{"i": 0., "p": 0.}])
    last_pivot_ref_buffer = np.array([{"i": 0., "p": 0.}])

    pivot_buffer = [] # np.ndarray((len(price) - lookback_depth, 6), dtype = dict)

    for bar_index in range(lookback_depth, len(price)):

        H = high_pivots(high_seq[bar_index-lookback_depth:bar_index], bar_index)
        L = low_pivots(low_seq[bar_index-lookback_depth:bar_index], bar_index)
        dev_thr = dev_threshold[bar_index]

        pivots, state = find_pivots(H, L, last_pivot, second_last_pivot, 
                                    last_line, last_pivot_direction, dev_thr, 
                                    second_last_pivot_ref_buffer, last_pivot_ref_buffer)

        if pivots not in pivot_buffer:
            pivot_buffer.append(pivots)

        last_pivot, second_last_pivot, last_line, last_pivot_direction, second_last_pivot_ref_buffer, last_pivot_ref_buffer = state

    return [[(a[0]["i"], a[0]["p"]), (a[1]["i"], a[1]["p"]), (a[2]["i"], a[2]["p"])] for a in pivot_buffer]


def get_pitchforks_hl(high_seq, low_seq, close_seq, lookback_depth, last_pivot_direction="high"):

    atr = ta.volatility.AverageTrueRange(high_seq, low_seq, close_seq, 10).average_true_range()
    dev_threshold = np.array(atr / close_seq * 100 * 3)
    high_seq = np.array(high_seq)
    low_seq = np.array(low_seq)
    last_pivot = {"i": 0., "p": 0.}
    second_last_pivot = {"i": 0., "p": 0.}
    last_line = [{"i": np.nan, "p": np.nan}, {"i": np.nan, "p": np.nan}]
    second_last_pivot_ref_buffer = np.array([{"i": 0., "p": 0.}])
    last_pivot_ref_buffer = np.array([{"i": 0., "p": 0.}])

    pivot_buffer = [] # np.ndarray((len(high_seq) - lookback_depth, 6), dtype = dict)

    for bar_index in range(lookback_depth, len(high_seq)):

        H = high_pivots(high_seq[bar_index-lookback_depth:bar_index], bar_index)
        L = low_pivots(low_seq[bar_index-lookback_depth:bar_index], bar_index)
        dev_thr = dev_threshold[bar_index]

        pivots, state = find_pivots(H, L, last_pivot, second_last_pivot,
                                    last_line, last_pivot_direction, dev_thr,
                                    second_last_pivot_ref_buffer, last_pivot_ref_buffer)

        if pivots not in pivot_buffer:
            pivot_buffer.append(pivots)

        last_pivot, second_last_pivot, last_line, last_pivot_direction, second_last_pivot_ref_buffer, last_pivot_ref_buffer = state

    return [[(a[0]["i"], a[0]["p"]), (a[1]["i"], a[1]["p"]), (a[2]["i"], a[2]["p"])] for a in pivot_buffer]

# ----------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    price = load_price()
    # price = price[-24*62:]
    price.index = range(len(price))
    all_pivots = get_pitchforks(price, 11)

    print(all_pivots)

    fig = plt.figure(figsize = (12, 8))
    ax = plt.subplot(111)

    ax.plot(price.index, price["bid_high"], c="Black", linewidth=0.3)
    ax.plot(price.index, price["bid_low"], c="Black", linewidth=0.3)

    for p in all_pivots[4:]:

        print(p)

        median_x, median_y = [p[0][0], (p[1][0] + p[2][0]) / 2], [p[0][1], (p[1][1] + p[2][1]) / 2]
        perp_x, perp_y = [p[1][0], p[1][0] + (p[2][0] - p[1][0])], [p[1][1], p[1][1] + (p[2][1] - p[1][1])]

        ax.plot(median_x, median_y, color="Red")
        ax.plot(perp_x, perp_y, color="Green")


    plt.show()
