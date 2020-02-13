library(alphavantager) #you need to install this package
av_api_key('YOUR_API_KEY') #you need a free API key from https://www.alphavantage.co/support/#api-key

intraday <- av_get('TSLA', av_fun = 'TIME_SERIES_INTRADAY', interval = '5min', outputsize = 'full')
  intraday$type <- 'intraday'
daily <- av_get('TSLA', av_fun = 'TIME_SERIES_DAILY', outputsize = 'full',time_period = 400, series_type = "close")
  daily$type <- 'daily'
weekly <- av_get('TSLA', av_fun = 'TIME_SERIES_WEEKLY', outputsize = 'full',time_period = 400, series_type = "close")
  weekly$type <- 'weekly'

all <- rbind(intraday,daily,weekly)
write.csv(all,"TSLA.csv",row.names = F)
