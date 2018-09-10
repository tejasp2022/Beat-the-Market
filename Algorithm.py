import numpy as np

from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import CustomFactor, AverageDollarVolume
from quantopian.pipeline.filters.morningstar import Q500US
from quantopian.pipeline.data.zacks import EarningsSurprises
from quantopian.pipeline.factors.zacks import BusinessDaysSinceEarningsSurprisesAnnouncement

# from quantopian.pipeline.data.accern import alphaone_free as alphaone
from quantopian.pipeline.data.accern import alphaone as alphaone

def make_pipeline(context):
    # Create our pipeline  
    pipe = Pipeline()  
    
    universe_filters = Q500US
    # Get PEADS Suprise factor
    factor = EarningsSurprises.eps_pct_diff_surp.latest
    # Get days since announcement
    days = BusinessDaysSinceEarningsSurprisesAnnouncement()
    # Get Sentiment Score
    article_sentiment = alphaone.article_sentiment.latest
  

    longs_factor = (factor >= 75) 
    shorts_factor = (factor <= -75)
    
    long_stocks = universe_filters() & longs_factor & article_sentiment.notnan() \
        & (article_sentiment >= 0.01)
    short_stocks = universe_filters() & shorts_factor & article_sentiment.notnan() \
        & (article_sentiment <= -0.01)

    # Add long/shorts to the pipeline  
    pipe.add(long_stocks, "longs")
    pipe.add(short_stocks, "shorts")
    pipe.add(days, 'days')
    pipe.add(article_sentiment, "sentiment")
    pipe.add(factor, "factor")
    pipe.set_screen(factor.notnan())
    return pipe  
        
def initialize(context):
    #: Set commissions 
    set_commission(commission.PerTrade(cost=6.95))
    set_slippage(slippage.VolumeShareSlippage(volume_limit=1.0, price_impact=0))
    #: Declaring the days to hold
    context.days_to_hold = 2
    #: Declares which stocks we currently held and how many days we've held them dict[stock:days_held]
    context.stocks_held = {}

    # Make our pipeline
    attach_pipeline(make_pipeline(context), 'earnings')

    
    # buy stocks long or short at market open
    schedule_function(func=order_positions,
                      date_rule=date_rules.every_day(),
                      time_rule=time_rules.market_open())
    
     # set up the stop loss sell strategy 60 minutes after buying
    schedule_function(func=setup_stoploss_orders,
                      date_rule=date_rules.every_day(),
                      time_rule=time_rules.market_open(minutes=60))

    # Log our positions before market closing
    schedule_function(func=log_positions,
                      date_rule=date_rules.every_day(),
                      time_rule=time_rules.market_close(minutes=30))



def before_trading_start(context, data):
    # Screen for securities that only have an earnings release
    # 1 business day previous and separate out the earnings surprises into
    # positive and negative 
    results = pipeline_output('earnings')
    results = results[results['days'] <= 4]
    results = results[results['days'] >= 1]
    
    assets_in_universe = results.index
    context.positive_surprise = assets_in_universe[results.longs]
    context.negative_surprise = assets_in_universe[results.shorts]
    
    # print to log the details of data pipe
    presults = results[results['longs']  | results['shorts'] ]
    if  len(presults) > 0:
        log.info(presults)
        
def order_positions(context, data):
    # order an equal percentage in each position
    cpt = 6.95 #commission per trade
    port = context.portfolio.positions
    record(leverage=context.account.leverage)
    NoOfLongs = len(context.positive_surprise.tolist())
    NoOfShorts = len(context.positive_surprise.tolist())
    TotStocks = NoOfLongs + NoOfShorts
    
    #find avaliable funds
    cash_to_buy = context.account.available_funds  - cpt * TotStocks
    #cash_to_buy = context.portfolio.cash - cpt * TotStocks 
    cash_per_stock = 0
    if (TotStocks) > 0:
        cash_per_stock = cash_to_buy/TotStocks
    
   
    # Check if we have stocks past our holding period
    for security in port:  
        if data.can_trade(security):  
             
            if context.stocks_held.get(security) is not None:  
                context.stocks_held[security] += 1  
                if context.stocks_held[security] >= context.days_to_hold:  
                    order_target_percent(security, 0)  
                    logInfo = "SD %s" %context.stocks_held[security]
                    log.info(logInfo) 
                    del context.stocks_held[security]
            # If we've deleted it but it still hasn't been exited. Try exiting again  
            else:  
                log.info("Haven't yet exited %s, ordering again" % security.symbol)  
                order_target_percent(security, 0)  
    
    # Buy stocks based on current pipeline and available cash
    for security in context.negative_surprise.tolist():
        current_price = data.current(security, 'price')
        NoOfShares = int(cash_per_stock/current_price)
        if data.can_trade(security):
            #order_value(security, -cash_per_stock)
            order(security,-NoOfShares)
            log.info("Buying {}*{}@${}=${}, commission ${}".format(NoOfShares, security.symbol,  
                  current_price, NoOfShares*current_price,  
                  cpt+NoOfShares))  
            if context.stocks_held.get(security) is None:
                context.stocks_held[security] = 0

                   
    for security in context.positive_surprise.tolist():
        
        if data.can_trade(security) :
            current_price =  data.current(security, 'price')
            NoOfShares = int(cash_per_stock/current_price)                                     
            #order_value(security, cash_per_stock)
            order(security,NoOfShares)
            log.info("Buying {} of {} @ $ {} = ${}, commission ${}".format(NoOfShares, security.symbol,  
                  current_price, NoOfShares*current_price,  
                  cpt))
                                                 
            if context.stocks_held.get(security) is None:
                context.stocks_held[security] = 0

                
def setup_stoploss_orders(context, data):
    #: Set up stop loss for today's orders.
    log_shorts = "\n"
    log_longs = "\n"
    
    for security in context.positive_surprise.tolist():
      
        if context.portfolio.positions[security].cost_basis > 0:
            SellPrice = context.portfolio.positions[security].cost_basis * 1.04
            order_value(security, 0, style=StopOrder(SellPrice))
            log_longs +=  "\in Long, %s, %s, %s, %s " % (security, context.portfolio.positions[security].amount,  context.portfolio.positions[security].cost_basis,  SellPrice)
            log.info(log_longs)        
            
    for security in context.negative_surprise.tolist():
      
         if context.portfolio.positions[security].cost_basis > 0:
            SellPrice = context.portfolio.positions[security].cost_basis * 0.96
            order_value(security, 0, style=StopOrder(SellPrice))
            
            log_shorts +=  "\n Short, %s, %s, %s, %s " % (security, context.portfolio.positions[security].amount,  context.portfolio.positions[security].cost_basis,  SellPrice)
            log.info(log_shorts)

                
def log_positions(context, data):
    #: Get all positions  
    if len(context.portfolio.positions) > 0:  
        all_positions = "\n" 
        for pos in context.portfolio.positions:  
            if context.portfolio.positions[pos].amount != 0:  
                all_positions += "%s , %s , %s \n " % (pos.symbol, context.portfolio.positions[pos].amount, context.portfolio.positions[pos].cost_basis)  
        log.info(all_positions)