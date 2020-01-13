from datetime import datetime
from concurrent import futures

import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web

def download_stock(stock):
	try:
		print(stock)
		stock_df = web.DataReader(stock,'yahoo', start_time, now_time)
		stock_df['Name'] = stock
		output_name = stock + '_data.csv'
		stock_df.to_csv("./stocks/"+output_name)
	except:
		bad_names.append(stock)
		print('bad: %s' % (stock))

if __name__ == '__main__':

	""" set the download window """
	now_time = datetime.now()
	start_time = datetime(now_time.year - 1, now_time.month , now_time.day)

	""" list of s_anp_p companies """
	s_and_p = ['TMUS','AWK' ,'JBHT','ODFL','URI','MO','PM','PBCT','AAPL','HPE','HPQ','NTAP','STX','WDC','XRX','CDW','FTNT','MSFT','NOW','NUE','AZO','KMX','GPC','ORLY','TSCO','ULTA','ALB','CE','DD','ECL','IFF','LYB','PPG','SHW','AMT','CCI','DLR','EQIX','EXR','IRM','PSA','SBAC','WY','HRB','KO','MNST','PEP','AMD','ADI','AVGO','INTC','MXIM','MCHP','MU','NVDA','QRVO','QCOM','SWKS','TXN','XLNX','AMAT','KLAC','LRCX','FRT','KIM','O','REG','SPG','CMG','DRI','MCD','SBUX','YUM','AIV','AVB','EQR','ESS','MAA','UDR','EFX','INFO','NLSN','VRSK','RE','CFG','FITB','FRC','HBAN','KEY','MTB','PNC','RF','SIVB','TFC','ZION','CBRE','CSX','KSU','NSC','UNP','NWSA','NWS','ALL','AIG','CB','CINF','HIG','PGR','TRV','WRB','ABBV','AGN','JNJ','LLY','MRK','MYL','PRGO','PFE','ZTS','COTY','EL','PG','AMCR','AVY','IP','PKG','SEE','WRK','CPB','CAG','GIS','HSY','HRL','SJM','K','KHC','LW','MKC','MDLZ','TSN','KMI','OKE','WMB','HFC','MPC','PSX','VLO','APA','COG','XEC','CXO','COP','DVN','FANG','EOG','MRO','NBL','OXY','PXD','BKR','HAL','NOV','SLB','FTI','HP','ARE','BXP','SLG','VNO','AEE','CNP','CMS','DTE','ES','EXC','NEE','NI','PNW','SRE','XEL','BRK-B','AIZ','LNC','L','FOXA','FOX','LYV','NFLX','VIAC','DIS','HOG','BLL','ANTM','CNC','CI','HUM','UNH','WCG','ILMN','IQV','MTD','TMO','AFL','GL','MET','PFG','PRU','UNM','HAS','ACN','CTSH','DXC','IT','IBM','LDOS','SCHW','ETFC','GS','MS','RJF','AKAM','ADP','VRSN','AMZN','BKNG','EBAY','EXPE','GOOGL','GOOG','FB','TWTR','ATVI','EA','TTWO','T','VZ','CVX','XOM','HES','AON','AJG','MMC','WLTW','DRE','PLD','CMI','DOV','FLS','FTV','GWW','IEX','ITW','IR','PH','PNR','SNA','SWK','XYL','APD','LIN','MMM','GE','HON','ROP','AES','NRG','COST','WMT','RHI','NWL','CHD','CLX','CL','KMB','WHR','CCL','HLT','MAR','NCLH','RCL','HST','DHI','LEN','NVR','PHM','HD','LOW','LEG','MHK','CERN','ALGN','COO','XRAY','CVS','LH','DGX','PEAK','VTR','WELL','DVA','HCA','UHS','ABT','ABMD','A','BAX','BDX','BSX','DHR','EW','HOLX','IDXX','ISRG','MDT','PKI','RMD','STE','SYK','TFX','VAR','ZBH','ABC','BMY','CAH','HSIC','MCK','WAT','NEM','DG','DLTR','KSS','TGT','ATO','KR','SYY','CBOE','CME','ICE','MKTX','MCO','MSCI','NDAQ','SPGI','CF','CTVA','FMC','MOS','RSG','ROL','WM','IPGP','TEL','FLIR','KEYS','ZBRA','APH','GLW','AME','ETN','EMR','ROK','LNT','AEP','ED','D','DUK','EIX','ETR','EVRG','FE','PPL','PEG','SO','WEC','WBA','CTAS','CPRT','EMN','BAC','C','CMA','JPM','USB','WFC','LKQ','BF-B','STZ','M','JWN','ADS','BR','FIS','FISV','FLT','GPN','JKHY','MA','PAYX','PYPL','V','WU','FCX','AXP','COF','DFS','SYF','GRMN','MLM','VMC','CAT','PCAR','WAB','J','PWR','BBY','ANET','CSCO','FFIV','JNPR','MSI','DOW','LVS','MGM','WYNN','CHTR','CMCSA','DISH','ALLE','AOS','FAST','FBHS','JCI','MAS','DISCA','DISCK','TAP','ALXN','AMGN','BIIB','GILD','INCY','REGN','VRTX','AAP','F','GM','APTV','BWA','AMP','BK','BLK','BEN','IVZ','NTRS','STT','TROW','ADBE','ANSS','ADSK','CDNS','CTXS','INTU','NLOK','ORCL','CRM','SNPS','CPRI','HBI','NKE','PVH','RL','TPR','TIF','UAA','UA','VFC','GPS','LB','ROST','TJX','CTL','ALK','AAL','DAL','LUV','UAL','CHRW','EXPD','FDX','UPS','ADM','DE','ARNC','BA','GD','HII','LHX','LMT','NOC','RTN','TXT','TDG','UTX','IPG','OMC']
		
	bad_names =[] #to keep track of failed queries

	"""here we use the concurrent.futures module's ThreadPoolExecutor
		to speed up the downloads buy doing them in parallel 
		as opposed to sequentially """

	#set the maximum thread number
	max_workers = 50

	workers = min(max_workers, len(s_and_p)) #in case a smaller number of stocks than threads was passed in
	with futures.ThreadPoolExecutor(workers) as executor:
		res = executor.map(download_stock, s_and_p)

	
	""" Save failed queries to a text file to retry """
	if len(bad_names) > 0:
		with open('failed_queries.txt','w') as outfile:
			for name in bad_names:
				outfile.write(name+'\n')
	
	#get market cap for all tickers
	market_cap = web.get_quote_yahoo(s_and_p)['marketCap']
	df = pd.DataFrame({'Name':market_cap.index, 'Market Cap':market_cap.values})
	df.to_csv("marketcap.csv", index=False)