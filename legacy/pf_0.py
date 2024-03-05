#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ta
import history
import numpy as np
import matplotlib.pyplot as plt
from copy import deepcopy


def high_pivots(src, state):
    l2 = state['depth']
    length = int(np.floor(l2/2.0))
    c = src[length]
    ok = True
    for i in range(0, l2):
        if src[i] > c:
            ok = False
    if ok:
        return [state['bar_index'] - length, c]
    else:
        return [np.nan, np.nan]

def low_pivots(src, state):
    l2 = state['depth']
    length = int(np.floor(l2/2.0))
    c = src[length]
    ok = True
    for i in range(0, l2):
        if src[i] < c:
            ok = False
    if ok:
        return [state['bar_index'] - length, c]
    else:
        return [np.nan, np.nan]


def calc_dev(base_price, price):
    return 100 * (price - base_price) / price


def pivot_found(dev, isHigh, index, price, state):
    lineLast_sum = np.sum(state['lineLast'])
    if state['isHighLast'] == isHigh and not np.isnan(lineLast_sum):
        # same direction
        check = True
        if state['isHighLast']:
            check = price > state['pLast']
        else:
            check = price < state['pLast']
        if check:
            state['lineLast'][2] = index
            state['lineLast'][3] = price
            return [state['lineLast'].copy(), state['isHighLast'], deepcopy(state)]
        else:
            return [[np.nan] * 4, np.nan, deepcopy(state)]
    else:  # reverse the direction (or create the very first line)
        if abs(dev) > state['dev_threshold'].iloc[state['bar_index']]:
            # price move is significant
            state['line'] = [state['iLast'], state['pLast'], index, price]
            return [state['line'].copy(), isHigh, deepcopy(state)]
        else:
            return [[np.nan] * 4, np.nan, deepcopy(state)]


def valuewhenchange(ref, occ):
    # Loop over list backwards
    occurrence = 0
    rev = ref.copy()
    rev.reverse()
    for l in range(len(rev) - 1):
        if rev[l] != rev[l + 1]:
            if occ == occurrence:
                return rev[l]
            occurrence += 1
    return rev[0]


def find_pivots(state):
    if not np.isnan(state['iH']):
        dev = calc_dev(state['pLast'], state['pH'])
        state['line'], isHigh, state = pivot_found(dev, True, state['iH'], state['pH'], state)
        line_sum = np.sum(np.array(state['line']))
        if not np.isnan(line_sum):
            if state['line'] != state['lineLast']:
                state['iPrevPivotRef'] = state['lineLast'][0]
                state['pPrevPivotRef'] = state['lineLast'][1]
                state['iLastPivotRef'] = state['lineLast'][2]
                state['pLastPivotRef'] = state['lineLast'][3]

                state['iPrevPivotRef_'].append(state['iPrevPivotRef'])
                state['pPrevPivotRef_'].append(state['pPrevPivotRef'])
                state['iLastPivotRef_'].append(state['iLastPivotRef'])
                state['pLastPivotRef_'].append(state['pLastPivotRef'])

            state['lineLast'] = state['line'].copy()
            state['isHighLast'] = isHigh
            state['iPrev'] = state['iLast']
            state['iLast'] = state['iH']
            state['pLast'] = state['pH']
    else:
        if not np.isnan(state['iL']):
            dev = calc_dev(state['pLast'], state['pL'])
            state['line'], isHigh, state = pivot_found(dev, False, state['iL'], state['pL'], state)

            line_sum = np.sum(np.array(state['line']))
            if not np.isnan(line_sum):
                if state['line'] != state['lineLast']:
                    state['iPrevPivotRef'] = state['lineLast'][0]
                    state['pPrevPivotRef'] = state['lineLast'][1]
                    state['iLastPivotRef'] = state['lineLast'][2]
                    state['pLastPivotRef'] = state['lineLast'][3]

                    state['iPrevPivotRef_'].append(state['iPrevPivotRef'])
                    state['pPrevPivotRef_'].append(state['pPrevPivotRef'])
                    state['iLastPivotRef_'].append(state['iLastPivotRef'])
                    state['pLastPivotRef_'].append(state['pLastPivotRef'])

                state['lineLast'] = state['line'].copy()
                state['isHighLast'] = isHigh
                state['iPrev'] = state['iLast']
                state['iLast'] = state['iL']
                state['pLast'] = state['pL']

    # print(iPrevPivotRef, pPrevPivotRef, iLastPivotRef, pLastPivotRef)

    iPrev2Pivot = valuewhenchange(state['iPrevPivotRef_'], state['i_histPivot'] + 1)
    pPrev2Pivot = valuewhenchange(state['pPrevPivotRef_'], state['i_histPivot'] + 1)
    iPrevPivot = valuewhenchange(state['iPrevPivotRef_'], state['i_histPivot'] + 0)
    pPrevPivot = valuewhenchange(state['pPrevPivotRef_'], state['i_histPivot'] + 0)
    iLastPivot = valuewhenchange(state['iLastPivotRef_'], state['i_histPivot'] + 0)
    pLastPivot = valuewhenchange(state['pLastPivotRef_'], state['i_histPivot'] + 0)
    #print(iPrev2Pivot, pPrev2Pivot, iPrevPivot, pPrevPivot, iLastPivot, pLastPivot)
    return [(iPrev2Pivot, pPrev2Pivot), (iPrevPivot, pPrevPivot), (iLastPivot, pLastPivot)], deepcopy(state)  # ,line.copy()



def get_pitchforks(price, lookback_depth):

    state = {}
    state['test'] = 0
    state['price'] = price
    state['bar_index'] = 0
    state['iH'] = 0
    state['pH'] = 0
    state['iL'] = 0
    state['pL'] = 0
    state['dev_threshold'] = 0
    state['depth'] = lookback_depth
    state['lineLast'] = [np.nan] * 4
    state['iLast'] = 0
    state['iPrev'] = 0
    state['pLast'] = 0
    state['isHighLast'] = False
    state['iPrevPivotRef'] = 0
    state['pPrevPivotRef'] = 0
    state['iLastPivotRef'] = 0
    state['pLastPivotRef'] = 0
    state['iPrevPivotRef_'] = [0]
    state['pPrevPivotRef_'] = [0]
    state['iLastPivotRef_'] = [0]
    state['pLastPivotRef_'] = [0]

    atr10 = ta.volatility.AverageTrueRange(price['bid_high'], price['bid_low'], price['bid_close'], 10)
    atr = atr10.average_true_range()
    state['dev_threshold'] = atr / price['bid_close'] * 100 * 3

    state['i_prevPivot'] = False
    state['i_histPivot'] = 0
    state['line'] = [np.nan] * 4

    pivots_l = []
    for j in range(11, len(price.index)):
        state['bar_index'] = j
        high = np.flip(np.array(price['bid_high'].iloc[:state['bar_index']]))
        low = np.flip(np.array(price['bid_low'].iloc[:state['bar_index']]))
        state['iH'], state['pH'] = high_pivots(high, state)
        state['iL'], state['pL'] = low_pivots(low, state)
        points, state = find_pivots(state)

        if points not in pivots_l:
            pivots_l.append(points)

    # print(state['test'])
    return pivots_l
    #print(medians)


if __name__ == '__main__':
    price = history.load_price()
    # price = price.iloc[-24 * 62:]
    # price.index = range(len(price))
    pivots = get_pitchforks(price, 11)

    print(pivots)
    
    fig = plt.figure(figsize = (12, 8))
    ax = plt.subplot(111)

    ax.plot(price.index, price["bid_high"], c="Black", linewidth=0.3)
    ax.plot(price.index, price["bid_low"], c="Black", linewidth=0.3)

    for p in pivots[4:]:

        print(p)

        median_x, median_y = [p[0][0], (p[1][0] + p[2][0]) / 2], [p[0][1], (p[1][1] + p[2][1]) / 2]
        perp_x, perp_y = [p[1][0], p[1][0] + (p[2][0] - p[1][0])], [p[1][1], p[1][1] + (p[2][1] - p[1][1])]

        ax.plot(median_x, median_y, color="Red")
        ax.plot(perp_x, perp_y, color="Green")

    plt.show()
