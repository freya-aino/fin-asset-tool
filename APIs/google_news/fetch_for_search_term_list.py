import GoogleNews as gn
import pandas as pd

# ----------------------------------------------------------------

days_to_record = 360
today = pd.to_datetime('today').date()
start_date = today - pd.to_timedelta(days_to_record, 'd')
stop_date = today
all_available_days = pd.DataFrame(pd.date_range(start_date, stop_date), columns=['Date'])

# ----------------------------------------------------------------

global_news_tables = {
    'Date': pd.DataFrame(),
    'Title': pd.DataFrame(),
    'Description': pd.DataFrame(),
    'Website': pd.DataFrame(),
    'Weekday': pd.DataFrame(),
    'Monthday': pd.DataFrame(),
    'Month': pd.DataFrame()
}

# ----------------------------------------------------------------

for (i, (_, row)) in enumerate(non_regional_syms.iterrows()):

    try:

        symbol = row['Symbol']
        name = row['Name']

        print('FETCH:', i, '/', len(non_regional_syms), ' :: ', symbol, ' : ', name)

        all_news = []

        google_news = gn.GoogleNews(lang='en')
        google_news.get_news('\"' + symbol + '\"')

        for news in google_news.results():
            date = news['datetime']

            if date != None:
                news = pd.Series({'Date':        date, 
                                  'Title':       news['title'], 
                                  'Description': news['desc'], 
                                  'Website':     news['site']})
            else:
                news = pd.Series({'Date':        None, 
                                  'Title':       news['title'], 
                                  'Description': news['desc'], 
                                  'Website':     news['site']})
            all_news.append(news)

            
        google_news.clear()
        google_news.get_news('\"' + name + '\"')

        for news in google_news.results():
            date = news['datetime']

            if date != None:
                news = pd.Series({'Date':        date, 
                                  'Title':       news['title'], 
                                  'Description': news['desc'], 
                                  'Website':     news['site']})
            else:
                news = pd.Series({'Date':        None, 
                                  'Title':       news['title'], 
                                  'Description': news['desc'], 
                                  'Website':     news['site']})
                all_news.append(news)
            

            
        stock_news_data = pd.DataFrame(all_news)
        
        stock_news_data = stock_news_data.sort_values('Date')
        stock_news_data.reset_index(inplace=True, drop=True)
        stock_news_data.reset_index(inplace=True)
        
        stock_news_data['Weekday'] = stock_news_data['Date'].dt.weekday
        stock_news_data['Monthday'] = stock_news_data['Date'].dt.day
        stock_news_data['Month'] = stock_news_data['Date'].dt.month
        
        stock_news_data['Date'] = stock_news_data['Date'].dt.strftime('%y-%m-%d')
        

        for k in global_news_tables:
            aa = dict(stock_news_data[k])
            aa['Symbol'] = symbol
            global_news_tables[k] = global_news_tables[k].append(aa, ignore_index=True)

    
        print('done')
        
    except Exception as e:
        print('WARNING:', e)
        continue
        