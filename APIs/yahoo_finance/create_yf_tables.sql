
create table if not exists hub_instrument (
    PK            text primary key,
    date_load     integer not null,
    record_source text not null,
    symbol        text not null,
    isin          text,
    unique(symbol)
);


create table if not exists sat_instrument_info_meta (
    PK_hub                        text not null references hub_instrument(PK),
    date_load                     integer not null,
    record_source                 text not null,
    symbol                        text,
    short_name                    text,
    long_name                     text,
    zip_code                      text,
    city                          text,
    phone_number                  text,
    sector                        text,
    long_business_summary         text,
    country                       text,
    address1                      text,
    address2                      text,
    industry                      text,
    full_time_employees           integer,
    gmt_offset_milliseconds       integer,
    quote_type                    text,
    market                        text,
    currency                      text,
    unique(PK_hub, date_load, record_source)
);

create table if not exists sat_instrument_info_technical (
    PK_hub                              text not null references hub_instrument(PK),
    date_load                           integer not null,
    record_source                       text not null,
    ebitda_margins                      real,
    beta                                real,
    held_percent_insiders               real,
    held_percent_institutions           real,
    profit_margins                      real,
    gross_margins                       real,
    operating_cashflow                  integer,
    revenue_growth                      real, 
    operating_margins                   real,
    market_cap                          integer,
    ebitda                              integer,
    target_low_price                    integer,
    target_median_price                 integer,
    target_mean_price                   integer,
    target_high_price                   integer,
    recommendation_mean                 real,
    recommendation_key                  text,
    gross_profits                       integer,
    free_cashflow                       integer,
    earnings_growth                     real,
    current_ratio                       real,
    return_on_assets                    real,
    number_of_analyst_opinions          integer,
    return_on_equity                    real,
    total_cash                          integer,
    total_debt                          integer,
    total_revenue                       integer,
    total_cash_per_share                real,
    revenue_per_share                   real,
    enterprise_value                    integer,
    shares_outstanding                  integer,
    last_fiscal_year_end                integer,
    next_fiscal_year_end                integer,
    last_dividend_value                 integer,
    float_shares                        integer,
    last_split_date                     integer,
    last_split_factor                   text,
    last_dividend_date                  integer,
    implied_shares_outstanding          integer,
    payout_ratio                        real,
    forward_eps                         real,
    book_value                          real,
    shares_short                        integer,
    yield                               integer,
    unique(PK_hub, date_load, record_source)
);

create table if not exists sat_instrument_info_metrics (
    PK_hub                              text not null references hub_instrument(PK),
    date_load                           integer not null,
    record_source                       text not null,
    fifty_two_week_change               real,
    beta_3_year                         real,
    sand_p_52_week_change               real,
    price_to_sales_trailing_12_months   real,
    enterprise_to_revenue               real,
    enterprise_to_ebitda                real,
    debt_to_equity                      real,
    peg_ratio                           real,
    quick_ratio                         real,
    net_income_to_common                integer,
    regular_market_open                 integer,
    two_hundred_day_average             integer,
    regular_market_day_high             integer,
    regular_market_previous_close       integer,
    fifty_day_average                   integer,
    average_daily_volume_10_day         integer,
    average_volume_10_days              integer,
    dividend_rate                       integer,
    ex_dividend_date                    integer,
    regular_market_day_low              integer,
    regular_market_volume               integer,
    average_volume                      integer,
    fifty_two_week_high                 integer,
    fifty_two_week_low                  integer,
    five_year_avg_dividend_yield        real,
    dividend_yield                      real,
    regularMarketPrice                  integer,
    trailing_annual_dividend_yield      real,
    trailing_annual_dividend_rate       real,
    pre_market_price                    real,
    trailing_eps                        real,
    trailing_pe                         real,
    forward_pe                          real,
    price_to_book                       real,
    short_ratio                         real,
    three_year_average_return           integer,
    five_year_average_return            integer,
    short_percentage_of_float           real,
    circulating_supply                  integer,
    unique(PK_hub, date_load, record_source)
);

create table if not exists sat_instrument_dividend (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    dividend      real,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_split (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    split         real,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_financials_year (
    PK_hub                                 text not null references hub_instrument(PK),
    date_load                              integer not null,
    record_source                          text not null,
    date                                   integer,
    research_development                   integer,
    effect_of_accounting_charges           integer,
    income_before_tax                      integer,
    net_income                             integer,
    selling_general_administrative         integer,
    gross_profit                           integer,
    ebit                                   integer,
    operating_income                       integer,
    other_operating_expenses               integer,
    interest_expense                       integer,
    income_tax_expense                     integer,
    total_revenue                          integer,
    total_operating_expenses               integer,
    cost_of_revenue                        integer,
    total_other_income_expense_net         integer,
    net_income_from_continuing_ops         integer,
    net_income_applicable_to_common_shares integer,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_financials_quarter (
    PK_hub                                          text not null references hub_instrument(PK),
    date_load                                       integer not null,
    record_source                                   text not null,
    date                                            integer,
    research_development                            integer,
    effect_of_accounting_charges                    integer,
    income_before_tax                               integer,
    net_income                                      integer,
    selling_general_administrative                  integer,
    gross_profit                                    integer,
    ebit                                            integer,
    operating_income                                integer,
    other_operating_expenses                        integer,
    interest_expense                                integer,
    income_tax_expense                              integer,
    total_revenue                                   integer,
    total_operating_expenses                        integer,
    cost_of_revenue                                 integer,
    total_other_income_expense_net                  integer,
    net_income_from_continuing_ops                  integer,
    net_income_applicable_to_common_shares          integer,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_general_holder_info (
    PK_hub                              text not null references hub_instrument(PK),
    date_load                           integer not null,
    record_source                       text not null,
    percent_shares_held_by_insider      real,
    percent_shares_held_institutions    real,
    percent_float_held_institutions     real,
    number_institutions_holding_shares  real,
    unique(PK_hub, date_load, record_source)
);

create table if not exists sat_instrument_institutional_holder (
    PK_hub          text not null references hub_instrument(PK),
    date_load       integer not null,
    record_source   text not null,
    holder          text,
    shares          integer,
    date_reported   integer,
    percent_out     real,
    value           integer,
    unique(PK_hub, record_source, holder, holder, date_reported)
);

create table if not exists sat_instrument_balance_sheet_year (
    PK_hub                    text not null references hub_instrument(PK),
    date_load                 integer not null,
    record_source             text not null,
    date                      integer,
    total_liab                integer,
    total_stockholder_equity  integer,
    other_current_liab        integer,
    total_assets              integer,
    common_stock              integer,
    other_current_assets      integer,
    retained_earnings         integer,
    other_liab                integer,
    treasury_stock            integer,
    other_assets              integer,
    cash                      integer,
    total_current_liabilities integer,
    short_long_term_debt      integer,
    other_stockholder_equity  integer,
    property_plant_equipment  integer,
    total_current_assets      integer,
    long_term_investments     integer,
    net_tangible_assets       integer,
    short_term_investments    integer,
    net_receivables           integer,
    long_term_debt            integer,
    inventory                 integer,
    accounts_payable          integer,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_balance_sheet_quarter (
    PK_hub                    text not null references hub_instrument(PK),
    date_load                 integer not null,
    record_source             text not null,
    date                      integer,
    total_liab                integer,
    total_stockholder_equity  integer,
    other_current_liab        integer,
    total_assets              integer,
    common_stock              integer,
    other_current_assets      integer,
    retained_earnings         integer,
    other_liab                integer,
    treasury_stock            integer,
    other_assets              integer,
    cash                      integer,
    total_current_liabilities integer,
    short_long_term_debt      integer,
    other_stockholder_equity  integer,
    property_plant_equipment  integer,
    total_current_assets      integer,
    long_term_investments     integer,
    net_tangible_assets       integer,
    short_term_investments    integer,
    net_receivables           integer,
    long_term_debt            integer,
    inventory                 integer,
    accounts_payable          integer,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_cashflow_year (
    PK_hub                                    text not null references hub_instrument(PK),
    date_load                                 integer not null,
    record_source                             text not null,
    date                                      integer,
    investments                               integer,
    change_to_liabilities                     integer,
    total_cashflows_from_investing_activities integer,
    net_borrowings                            integer,  
    total_cash_from_financing_activities      integer,
    change_to_operating_activities            integer,
    issuance_of_stock                         integer,
    net_income                                integer,
    change_in_cash                            integer,
    repurchase_of_stock                       integer,
    total_cash_from_operating_activities      integer,
    depreciation                              integer,
    other_cashflows_from_investing_activities integer,
    dividends_paid                            integer,
    change_to_inventory                       integer,
    change_to_account_receivables             integer,
    other_cashflows_from_financing_activities integer,
    change_to_net_income                      integer, 
    capital_expenditures                      integer,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_cashflow_quarter (
    PK_hub                                    text not null references hub_instrument(PK),
    date_load                                 integer not null,
    record_source                             text not null,
    date                                      integer,
    investments                               integer,
    change_to_liabilities                     integer,
    total_cashflows_from_investing_activities integer,
    net_borrowings                            integer,  
    total_cash_from_financing_activities      integer,
    change_to_operating_activities            integer,
    issuance_of_stock                         integer,
    net_income                                integer,
    change_in_cash                            integer,
    repurchase_of_stock                       integer,
    total_cash_from_operating_activities      integer,
    depreciation                              integer,
    other_cashflows_from_investing_activities integer,
    dividends_paid                            integer,
    change_to_inventory                       integer,
    change_to_account_receivables             integer,
    other_cashflows_from_financing_activities integer,
    change_to_net_income                      integer, 
    capital_expenditures                      integer,
    unique(PK_hub, record_source, date)
);


create table if not exists sat_instrument_earnings_year (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    year          text,
    revenue       integer,
    earnings      integer,
    unique(PK_hub, record_source, year)
);


create table if not exists sat_instrument_earnings_quarter (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    quarter       text,
    revenue       integer,
    earnings      integer,
    unique(PK_hub, record_source, quarter)
);

create table if not exists sat_instrument_sustainability (
    PK_hub                text not null references hub_instrument(PK),
    date_load             integer not null,
    record_source         text not null,
    social_score          real,
    governance_score      real,
    environment_score     real,
    highest_controversy   real,
    percentile            real,
    total_esg             real,
    peer_count            real,
    esg_performance       text,
    peer_group            text,
    palm_oil              integer check(palm_oil = 0 or palm_oil = 1),
    controversial_weapons integer check(controversial_weapons = 0 or controversial_weapons = 1),
    gambling              integer check(gambling = 0 or gambling = 1),
    nuclear               integer check(nuclear = 0 or nuclear = 1),
    fur_leather           integer check(fur_leather = 0 or fur_leather = 1),
    alcoholic             integer check(alcoholic = 0 or alcoholic = 1),
    gmo                   integer check(gmo = 0 or gmo = 1),
    catholic              integer check(catholic = 0 or catholic = 1),
    animal_testing        integer check(animal_testing = 0 or animal_testing = 1),
    tobacco               integer check(tobacco = 0 or tobacco = 1),
    coal                  integer check(coal = 0 or coal = 1),
    pesticides            integer check(pesticides = 0 or pesticides = 1),
    adult                 integer check(adult = 0 or adult = 1),
    small_arms            integer check(small_arms = 0 or small_arms = 1),
    military_contract     integer check(military_contract = 0 or military_contract = 1),
    unique(PK_hub, date_load, record_source)
);

create table if not exists sat_instrument_recomendation (
    PK_hub          text not null references hub_instrument(PK),
    date_load       integer not null,
    record_source   text not null,
    date            integer,
    firm            text,
    from_grade      text,
    to_grade        text,
    action          text,
    unique(PK_hub, record_source, date, firm)
);

create table if not exists sat_instrument_calendar (
    PK_hub              text not null references hub_instrument(PK),
    date_load           integer not null,
    record_source       text not null,
    earnings_date       integer,
    earnings_average    real,
    earnings_low        real,
    earnings_high       real,
    revenue_average     integer,
    revenue_low         integer,
    revenue_high        integer,
    unique(PK_hub, record_source, earnings_date)
);

create table if not exists sat_instrument_news (
    PK_hub                  text not null references hub_instrument(PK),
    date_load               integer not null,
    record_source           text not null,
    provider_publish_time   integer,
    title                   text,
    publisher               text,
    type                    text,
    unique(PK_hub, record_source, provider_publish_time)
);

create table if not exists sat_instrument_symbol_1d (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    open          real,
    high          real,
    low           real,
    close         real,
    volume        integer,
    dividend      real,
    stock_split   real,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_symbol_1h (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    open          real,
    high          real,
    low           real,
    close         real,
    volume        integer,
    dividend      real,
    stock_split   real,
    unique(PK_hub, record_source, date)
);


create table if not exists sat_instrument_symbol_5m (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    open          real,
    high          real,
    low           real,
    close         real,
    volume        integer,
    dividend      real,
    stock_split   real,
    unique(PK_hub, record_source, date)
);
