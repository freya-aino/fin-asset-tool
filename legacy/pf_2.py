from copy import deepcopy
from typing import Sequence
import numpy as np
import ta
from history import load_price
import time
import matplotlib.pyplot as plt


def get_pitchforks(price, lookback_depth, consecutive_same_direction_regression_steps = 3, number_of_relaxation_steps = 0):

    high_seq = np.array(price["bid_high"])
    low_seq = np.array(price["bid_low"])

    # atr = ta.volatility.AverageTrueRange(price['bid_high'], price['bid_low'], price['bid_close'], 10).average_true_range()
    # dev_threshold = np.array(atr / price['bid_close']) * dev_thr_factor


    global_high = np.stack([np.concatenate([high_seq[i:], [high_seq[-1]] * i], axis=0) for i in range(lookback_depth)])
    global_high_i = np.arange(global_high.shape[1]) + global_high.argmax(axis=0)
    global_high_p = global_high.max(axis=0)
    global_high_pivots_mask = global_high.argmax(axis=0) == int(lookback_depth/2)
    local_high_pivots_i = global_high_i[global_high_pivots_mask]
    local_high_pivots_p = global_high_p[global_high_pivots_mask]

    global_low = np.stack([np.concatenate([low_seq[i:], [low_seq[-1]] * i], axis=0) for i in range(lookback_depth)])
    global_low_i = np.arange(global_low.shape[1]) + global_low.argmin(axis=0)
    global_low_p = global_low.min(axis=0)
    global_low_pivots_mask = global_low.argmin(axis=0) == int(lookback_depth / 2)
    local_low_pivots_i = global_low_i[global_low_pivots_mask]
    local_low_pivots_p = global_low_p[global_low_pivots_mask]


    # from_low_last_pivot_is_high = np.array([np.concatenate([[np.nan], local_high_pivots_i[local_high_pivots_i < i]])[-1] > np.concatenate([[np.nan], local_low_pivots_i[local_low_pivots_i < i]])[-1] for i in local_low_pivots_i])
    # from_high_last_pivot_is_low = np.array([np.concatenate([[np.nan], local_low_pivots_i[local_low_pivots_i < i]])[-1] > np.concatenate([[np.nan], local_high_pivots_i[local_high_pivots_i < i]])[-1] for i in local_high_pivots_i])


    # low_after_switch_i = local_low_pivots_i # [from_low_last_pivot_is_high]
    # low_after_switch_p = local_low_pivots_p # [from_low_last_pivot_is_high]
    # low_after_switch_previous_i = np.array([np.concatenate([[np.nan], local_high_pivots_i[local_high_pivots_i < i]])[-1] for i in low_after_switch_i])
    # low_after_switch_previous_p = np.array([local_high_pivots_p[local_high_pivots_i == i][0] for i in low_after_switch_previous_i])

    # low_dev = (low_after_switch_p - low_after_switch_previous_p) / low_after_switch_p * dev_factor
    # low_dev_mask = np.abs(low_dev) > np.array([dev_threshold[i] for i in low_after_switch_i])
    # low_after_switch_i = low_after_switch_i[low_dev_mask]
    # low_after_switch_p = low_after_switch_p[low_dev_mask]


    # high_after_switch_i = local_high_pivots_i # [from_high_last_pivot_is_low]
    # high_after_switch_p = local_high_pivots_p # [from_high_last_pivot_is_low]
    # high_after_switch_previous_i = np.array([np.concatenate([[np.nan], local_low_pivots_i[local_low_pivots_i < i]])[-1] for i in high_after_switch_i])
    # high_after_switch_previous_p = np.array([local_low_pivots_p[local_low_pivots_i == i][0] for i in high_after_switch_previous_i])

    # high_dev = (high_after_switch_p - high_after_switch_previous_p) / high_after_switch_p * dev_factor
    # high_dev_mask = np.abs(high_dev) > np.array([dev_threshold[i] for i in high_after_switch_i])
    # high_after_switch_i = high_after_switch_i[high_dev_mask]
    # high_after_switch_p = high_after_switch_p[high_dev_mask]



    for _ in range(consecutive_same_direction_regression_steps):

        # check if next low pivot is the same direction and lower
        low_next_is_low = np.array([np.concatenate([local_low_pivots_i[local_low_pivots_i > i], [np.nan]])[0] < np.concatenate([local_high_pivots_i[local_high_pivots_i >= i], [local_low_pivots_i.max() + 1]])[0] for i in local_low_pivots_i])
        low_next_is_lower = np.array([np.concatenate([local_low_pivots_p[local_low_pivots_i > i], [np.nan]])[0] < local_low_pivots_p[local_low_pivots_i == i][0] for i in local_low_pivots_i])

        local_low_pivots_p[low_next_is_low & low_next_is_lower] = np.array([local_low_pivots_p[local_low_pivots_i > i][0] for i in local_low_pivots_i[low_next_is_low & low_next_is_lower]])
        local_low_pivots_i[low_next_is_low & low_next_is_lower] = np.array([local_low_pivots_i[local_low_pivots_i > i][0] for i in local_low_pivots_i[low_next_is_low & low_next_is_lower]])
        
        local_low_pivots_p = local_low_pivots_p[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]
        local_low_pivots_i = local_low_pivots_i[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]

        # check if next high pivot is the same direction and higher
        high_next_is_high = np.array([np.concatenate([local_high_pivots_i[local_high_pivots_i > i], [np.nan]])[0] < np.concatenate([local_low_pivots_i[local_low_pivots_i >= i], [local_high_pivots_i.max() + 1]])[0] for i in local_high_pivots_i])
        high_next_is_higher = np.array([np.concatenate([local_high_pivots_p[local_high_pivots_i > i], [np.nan]])[0] > local_high_pivots_p[local_high_pivots_i == i][0] for i in local_high_pivots_i])

        local_high_pivots_p[high_next_is_high & high_next_is_higher] = np.array([local_high_pivots_p[local_high_pivots_i > i][0] for i in local_high_pivots_i[high_next_is_high & high_next_is_higher]])
        local_high_pivots_i[high_next_is_high & high_next_is_higher] = np.array([local_high_pivots_i[local_high_pivots_i > i][0] for i in local_high_pivots_i[high_next_is_high & high_next_is_higher]])

        local_high_pivots_p = local_high_pivots_p[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]
        local_high_pivots_i = local_high_pivots_i[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]


        # check if previous low pivot is the same direction and lower
        low_prev_is_low = np.array([np.concatenate([[np.nan], local_low_pivots_i[local_low_pivots_i < i]])[-1] > np.concatenate([[-1], local_high_pivots_i[local_high_pivots_i <= i]])[-1] for i in local_low_pivots_i])
        low_prev_is_lower = np.array([np.concatenate([[np.nan], local_low_pivots_p[local_low_pivots_i < i]])[-1] < local_low_pivots_p[local_low_pivots_i == i][0] for i in local_low_pivots_i])

        local_low_pivots_p[low_prev_is_low & low_prev_is_lower] = np.array([local_low_pivots_p[local_low_pivots_i < i][-1] for i in local_low_pivots_i[low_prev_is_low & low_prev_is_lower]])
        local_low_pivots_i[low_prev_is_low & low_prev_is_lower] = np.array([local_low_pivots_i[local_low_pivots_i < i][-1] for i in local_low_pivots_i[low_prev_is_low & low_prev_is_lower]])

        local_low_pivots_p = local_low_pivots_p[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]
        local_low_pivots_i = local_low_pivots_i[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]

        # check if previous high pivot is the same direction and higher
        high_prev_is_high = np.array([np.concatenate([[np.nan], local_high_pivots_i[local_high_pivots_i < i]])[-1] > np.concatenate([[-1], local_low_pivots_i[local_low_pivots_i <= i]])[-1] for i in local_high_pivots_i])
        high_prev_is_higher = np.array([np.concatenate([[np.nan], local_high_pivots_p[local_high_pivots_i < i]])[-1] > local_high_pivots_p[local_high_pivots_i == i][0] for i in local_high_pivots_i])
        
        local_high_pivots_p[high_prev_is_high & high_prev_is_higher] = np.array([local_high_pivots_p[local_high_pivots_i < i][-1] for i in local_high_pivots_i[high_prev_is_high & high_prev_is_higher]])
        local_high_pivots_i[high_prev_is_high & high_prev_is_higher] = np.array([local_high_pivots_i[local_high_pivots_i < i][-1] for i in local_high_pivots_i[high_prev_is_high & high_prev_is_higher]])

        local_high_pivots_p = local_high_pivots_p[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]
        local_high_pivots_i = local_high_pivots_i[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]


    # -----------------------------------------------------------
    # take the min (low) and max (high) for two adjacent pivots (high and low beeing considered seperatly)
    # do this "number_of_relaxation_steps" number of times

    for _ in range(number_of_relaxation_steps):

        lp_1 = local_low_pivots_p[range(0, len(local_low_pivots_p), 2)]
        lp_2 = local_low_pivots_p[range(1, len(local_low_pivots_p), 2)]
        li_1 = local_low_pivots_i[range(0, len(local_low_pivots_p), 2)]
        li_2 = local_low_pivots_i[range(1, len(local_low_pivots_p), 2)]
        lp_min = np.min([len(lp_1), len(lp_2), len(li_1), len(li_2)])
        low_p_dt_stack = np.stack([lp_1[:lp_min], lp_2[:lp_min]])
        low_i_dt_stack = np.stack([li_1[:lp_min], li_2[:lp_min]])
        
        low_range_argmin = np.expand_dims(low_p_dt_stack.argmin(axis = 0), axis=0)

        local_low_pivots_p = np.take_along_axis(low_p_dt_stack, low_range_argmin, axis=0).flatten()
        local_low_pivots_i = np.take_along_axis(low_i_dt_stack, low_range_argmin, axis=0).flatten()

        local_low_pivots_p = local_low_pivots_p[np.concatenate([local_low_pivots_i[1:] != local_low_pivots_i[:-1], [True]])]
        local_low_pivots_i = local_low_pivots_i[np.concatenate([local_low_pivots_i[1:] != local_low_pivots_i[:-1], [True]])]


        hp_1 = local_high_pivots_p[range(0, len(local_high_pivots_p), 2)]
        hp_2 = local_high_pivots_p[range(1, len(local_high_pivots_p), 2)]
        hi_1 = local_high_pivots_i[range(0, len(local_high_pivots_p), 2)]
        hi_2 = local_high_pivots_i[range(1, len(local_high_pivots_p), 2)]
        hp_min = np.min([len(hp_1), len(hp_2), len(hi_1), len(hi_2)])
        high_p_dt_stack = np.stack([hp_1[:hp_min], hp_2[:hp_min]])
        high_i_dt_stack = np.stack([hi_1[:hp_min], hi_2[:hp_min]])
        
        high_range_argmax = np.expand_dims(high_p_dt_stack.argmax(axis = 0), axis=0)

        local_high_pivots_p = np.take_along_axis(high_p_dt_stack, high_range_argmax, axis=0).flatten()
        local_high_pivots_i = np.take_along_axis(high_i_dt_stack, high_range_argmax, axis=0).flatten()

        local_high_pivots_p = local_high_pivots_p[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]
        local_high_pivots_i = local_high_pivots_i[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]


        # --------------------------------------------------------
        # clean again

        # check if next low pivot is the same direction and lower
        low_next_is_low = np.array([np.concatenate([local_low_pivots_i[local_low_pivots_i > i], [np.nan]])[0] < np.concatenate([local_high_pivots_i[local_high_pivots_i >= i], [local_low_pivots_i.max() + 1]])[0] for i in local_low_pivots_i])
        low_next_is_lower = np.array([np.concatenate([local_low_pivots_p[local_low_pivots_i > i], [np.nan]])[0] < local_low_pivots_p[local_low_pivots_i == i][0] for i in local_low_pivots_i])

        local_low_pivots_p[low_next_is_low & low_next_is_lower] = np.array([local_low_pivots_p[local_low_pivots_i > i][0] for i in local_low_pivots_i[low_next_is_low & low_next_is_lower]])
        local_low_pivots_i[low_next_is_low & low_next_is_lower] = np.array([local_low_pivots_i[local_low_pivots_i > i][0] for i in local_low_pivots_i[low_next_is_low & low_next_is_lower]])
        
        local_low_pivots_p = local_low_pivots_p[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]
        local_low_pivots_i = local_low_pivots_i[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]

        # check if next high pivot is the same direction and higher
        high_next_is_high = np.array([np.concatenate([local_high_pivots_i[local_high_pivots_i > i], [np.nan]])[0] < np.concatenate([local_low_pivots_i[local_low_pivots_i >= i], [local_high_pivots_i.max() + 1]])[0] for i in local_high_pivots_i])
        high_next_is_higher = np.array([np.concatenate([local_high_pivots_p[local_high_pivots_i > i], [np.nan]])[0] > local_high_pivots_p[local_high_pivots_i == i][0] for i in local_high_pivots_i])

        local_high_pivots_p[high_next_is_high & high_next_is_higher] = np.array([local_high_pivots_p[local_high_pivots_i > i][0] for i in local_high_pivots_i[high_next_is_high & high_next_is_higher]])
        local_high_pivots_i[high_next_is_high & high_next_is_higher] = np.array([local_high_pivots_i[local_high_pivots_i > i][0] for i in local_high_pivots_i[high_next_is_high & high_next_is_higher]])

        local_high_pivots_p = local_high_pivots_p[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]
        local_high_pivots_i = local_high_pivots_i[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]


        # check if previous low pivot is the same direction and lower
        low_prev_is_low = np.array([np.concatenate([[np.nan], local_low_pivots_i[local_low_pivots_i < i]])[-1] > np.concatenate([[-1], local_high_pivots_i[local_high_pivots_i <= i]])[-1] for i in local_low_pivots_i])
        low_prev_is_lower = np.array([np.concatenate([[np.nan], local_low_pivots_p[local_low_pivots_i < i]])[-1] < local_low_pivots_p[local_low_pivots_i == i][0] for i in local_low_pivots_i])

        local_low_pivots_p[low_prev_is_low & low_prev_is_lower] = np.array([local_low_pivots_p[local_low_pivots_i < i][-1] for i in local_low_pivots_i[low_prev_is_low & low_prev_is_lower]])
        local_low_pivots_i[low_prev_is_low & low_prev_is_lower] = np.array([local_low_pivots_i[local_low_pivots_i < i][-1] for i in local_low_pivots_i[low_prev_is_low & low_prev_is_lower]])

        local_low_pivots_p = local_low_pivots_p[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]
        local_low_pivots_i = local_low_pivots_i[np.concatenate([[True], local_low_pivots_i[1:] != local_low_pivots_i[:-1]])]

        # check if previous high pivot is the same direction and higher
        high_prev_is_high = np.array([np.concatenate([[np.nan], local_high_pivots_i[local_high_pivots_i < i]])[-1] > np.concatenate([[-1], local_low_pivots_i[local_low_pivots_i <= i]])[-1] for i in local_high_pivots_i])
        high_prev_is_higher = np.array([np.concatenate([[np.nan], local_high_pivots_p[local_high_pivots_i < i]])[-1] > local_high_pivots_p[local_high_pivots_i == i][0] for i in local_high_pivots_i])
        
        local_high_pivots_p[high_prev_is_high & high_prev_is_higher] = np.array([local_high_pivots_p[local_high_pivots_i < i][-1] for i in local_high_pivots_i[high_prev_is_high & high_prev_is_higher]])
        local_high_pivots_i[high_prev_is_high & high_prev_is_higher] = np.array([local_high_pivots_i[local_high_pivots_i < i][-1] for i in local_high_pivots_i[high_prev_is_high & high_prev_is_higher]])

        local_high_pivots_p = local_high_pivots_p[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]
        local_high_pivots_i = local_high_pivots_i[np.concatenate([local_high_pivots_i[1:] != local_high_pivots_i[:-1], [True]])]

    # --------------------------------------------------------------
    # take all pivots in order of their indecies
    # remove indecies at the beginning to fit sets of three pivots
    # construct pitchfork (sets of three pivots)

    all_i = np.concatenate([local_low_pivots_i, local_high_pivots_i])
    all_p = np.concatenate([local_low_pivots_p, local_high_pivots_p])
    sorted_indecies = np.argsort(all_i)
    sequential_pivots = np.stack([np.take(all_i, sorted_indecies), np.take(all_p, sorted_indecies)])

    sequential_pivots = sequential_pivots[:, -(sequential_pivots.shape[1] - (sequential_pivots.shape[1] % 3)):]

    pivots_a = sequential_pivots[:, range(0, sequential_pivots.shape[1], 3)]
    pivots_b = sequential_pivots[:, range(1, sequential_pivots.shape[1], 3)]
    pivots_c = sequential_pivots[:, range(2, sequential_pivots.shape[1], 3)]

    # print(pivots_a.shape, pivots_b.shape, pivots_c.shape)

    pivots = np.stack([pivots_a, pivots_b, pivots_c])

    return [[(pivots[0, 0, i], pivots[0, 1, i]), (pivots[1, 0, i], pivots[1, 1, i]), (pivots[2, 0, i], pivots[2, 1, i])] for i in range(pivots.shape[-1])]


    

# ----------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    price = load_price()
    # price = price[-24*62:]
    price.index = range(len(price))



    dt = time.time()

    pivots = get_pitchforks(price, 5)
    print(time.time() - dt)
    
    fig = plt.figure(figsize = (12, 8))
    ax = plt.subplot(111)
    
    ax.plot(price.index, price["bid_high"], c="Black", linewidth=0.3)
    ax.plot(price.index, price["bid_low"], c="Black", linewidth=0.3)

    # ax.scatter(local_high_pivots_i, local_high_pivots_p, color = "orange")
    # ax.scatter(local_low_pivots_i, local_low_pivots_p, color="blue")

    for p in pivots:

        print(p)

        median_x, median_y = [p[0][0], (p[1][0] + p[2][0]) / 2], [p[0][1], (p[1][1] + p[2][1]) / 2]
        perp_x, perp_y = [p[1][0], p[1][0] + (p[2][0] - p[1][0])], [p[1][1], p[1][1] + (p[2][1] - p[1][1])]

        ax.plot(median_x, median_y, color="Red")
        ax.plot(perp_x, perp_y, color="Green")


    plt.show()
