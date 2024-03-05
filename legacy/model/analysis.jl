


symbols = []


struct SymbolGeneralInfo
    industry::String
    country::String
    sector::String
    num_employees::Int32
end

struct SymbolQuaterlyUpdate
    earnings_per_share::Float32
    PE::Float32
    gross_profit::Float32
    total_revenue::Float32
    income_before_tax::Float32
    EBIT::Float32
    EBITDA::Float32 # ?
    net_income::Float32
    operating_income::Float32
    total_operating_expense::Float32
    income_tax_expense::Float32
    cost_of_revenue::Float32
    market_cap::Float32
    divident_yield::Float32
    total_dept::Float32
    total_cash::Float32
    total_cash_per_share::Float32
    revenue_per_share::Float32
    shares_outstanding::Int32
    
    profit_margins::Float32
    gross_margins::Float32
    beta::Float32
    revenue_growth::Float32
    gross_profts::Float32
    free_cashflow::Float32

    target_min_price::Float32
    target_median_price::Float32
    target_mean_price::Float32
    target_max_price::Float32

    enterprise_value::Float32    

    best_correlating_symbols::AbstractArray{String}
end

struct SymbolDailyUpdate

    flat_variance::Float32
    delta_variance::Float32
    buy_volume::Float32
    sell_volume::Float32
    pivots::AbstractArray{Integer} # day index (probably 5min)
    day_start_jump::Float32 # bc other markets

    linear_trend::Float32
    sharp_changes::AbstractArray{Pair{Integer, Float32}} # (day index, amount)
    plateus::AbstractArray{Pair{Integer, Integer}} # (day index, index length)

    # TODO: stocks_helt::AbstractArray
    
    linear_tend_coming_into_the_day::Float32
    # TODO: fourier_trend_coming_into_the_day::Fourier

    news::AbstractArray{AbstractArray{String}}
    sentiment::Float32

    covariance_to_related_today::AbstractArray{Float32} # array of related
    covariance_to_related_prior_days::AbstractArray{Float32, 2} # for each day (total) [asking how much daily trends correlated over the past n days]

end

struct SymbolDailyInfo
    DoW_variance::AbstractArray{Float32, 2}
    DoM_variance::AbstractArray{Float32, 2}
    DoW_buy_volume::AbstractArray{Float32, 2}
    DoW_sell_volume::AbstractArray{Float32, 2}
    DoM_buy_volume::AbstractArray{Float32, 2}
    DoM_sell_volume::AbstractArray{Float32, 2}
end


struct SymbolHourlyUpdate
    buy_volume::Float32
    sell_volume::Float32
    variance::Float32

    linear_trend::Float32

    news::AbstractArray{AbstractArray{String}}
    sentiment::Float32

    covariance_to_related_current::AbstractArray{Float32} # array of related
    covariance_to_related_prior_hours::AbstractArray{Float32, 2} # for each hour since start of day
end

struct SymbolHourlyInfo
    HoD_buy_volume::AbstractArray{Float32, 2}
    HoD_sell_volume::AbstractArray{Float32, 2}
    HoD_variance::AbstractArray{Float32, 2}
end


struct NonTickDeltaUpdate
    open::Float32
    high::Float32
    low::Float32
    close::Float32
    volume::Float32
end


function find_best_correlating_symbols(symbol::String)
end


# function simulate_investors() 
#     # greedy: assuming everyone sells at a profit
# end


params = Dict([
    "plateu_threshold" => 0.1
])




