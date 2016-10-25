#' alpha portfolio demo data
#'
#' part of index EI000905's alpha portfolio data set.
#' @format A data frame with 1149 rows and 3 variables.
"portdemo"


#' assets return demo dataset.
#'
#' A dataset containing stock index(000985.CSI), bond index(037.CS) and commodity(GC00.CMX) daily return data since 2009.
#'
#' @format A data frame with 2865 rows and 4 variables:
#' \describe{
#'   \item{date}{date type}
#'   \item{stock}{stock index return}
#'   \item{bond}{bond index return}
#'   \item{commodity}{commodity index return}
#' }
"rtndemo"


#' bracket a series of string
#'
#'
#' @author Andrew Dow
#' @param series is a series object
#' @return a string object with bracket surrounded the series object.
#' @examples
#' series <- c('EQ000001','EQ000002')
#' brkQT(series)
#' @export
brkQT <- function(series){
  tmp <- paste(series,collapse = "','")
  tmp <- paste("('",tmp,"')",sep='')
  return(tmp)
}



#' bank rotation
#'
#' This is a bank stocks rotation strategy.Its idea comes from
#' \url{https://www.jisilu.cn/question/50176}.
#'
#' @author Andrew Dow
#' @param begDate is strategy's begin date
#' @param endDate is strategy's end date
#' @param chgBar is rotation's bar
#' @return a list of newest bank info and strategy's historical return.
#' @examples
#' begDate <- as.Date('2014-01-04')
#' endDate <- Sys.Date()-1
#' chgBar <- 0.2
#' bankport <- bank.rotation(begDate,endDate,chgBar)
#' @export
bank.rotation <- function(begDate,endDate=Sys.Date(),chgBar=0.2){

  #get TSF
  rebDates <- getRebDates(begDate,endDate,rebFreq = 'day')
  TS <- getTS(rebDates,indexID = 'ES09440100')
  TSF <- gf.PB_mrq_new(TS,datasource='quant')
  tmp <- gf.F_ROE_new(TS,datasource='cs')
  TSF <- merge(TSF,tmp,by=c('date','stockID'),all.x=T)
  TSF$stockName <- stockID2name(TSF$stockID)
  colnames(TSF) <- c("date","stockID","PB_mrq_","F_ROE_1","stockName")
  TSF <- TSF[,c("date","stockID","stockName","PB_mrq_","F_ROE_1")]
  TSF <- na.omit(TSF)
  TSF$factorscore <- log(TSF$PB_mrq_*2,base=(1+TSF$F_ROE_1))

  #get bank
  dates <- unique(TSF$date)
  bankPort <- data.frame()
  for(i in 1:length(dates)){
    if(i==1){
      tmp <- TSF[TSF$date==dates[i],]
      tmp <- plyr::arrange(tmp,factorscore)
      bankPort <- rbind(bankPort,tmp[1,])
    }else{
      tmp <- TSF[TSF$date==dates[i],]
      tmp <- plyr::arrange(tmp,factorscore)
      tmp.stock <- bankPort$stockID[nrow(bankPort)]
      if(tmp.stock %in% tmp$stockID){
        if(tmp[1,'factorscore']<tmp[tmp$stockID==tmp.stock,'factorscore']*(1-chgBar)){
          bankPort <- rbind(bankPort,tmp[1,])
        }else{
          bankPort <- rbind(bankPort,tmp[tmp$stockID==tmp.stock,])
        }
      }else{
        bankPort <- rbind(bankPort,tmp[1,])
      }
    }
  }

  #get bank daily return
  TSR <- bankPort[,c("date","stockID")]
  TSR$date <- c(TSR$date[-1],NA)
  TSR <- na.omit(TSR)
  tmp <- unique(TSR$stockID)
  tmp <- brkQT(substr(tmp,3,8))
  qr <-paste("SELECT convert(varchar,TradingDay,112) 'date','EQ'+s.SecuCode 'stockID',
             ClosePrice/PrevClosePrice-1 'periodrtn'
             FROM QT_DailyQuote q,SecuMain s
             where s.SecuCategory=1 and s.SecuMarket in(83,90)
             and q.InnerCode=s.InnerCode and s.SecuCode in",tmp,
             "and q.TradingDay>=",QT(min(TSR$date)),
             " and q.TradingDay<=",QT(max(TSR$date)) )
  con <- db.jy()
  re <- sqlQuery(con,qr)
  re$date <- intdate2r(re$date)
  TSR <- merge.x(TSR,re)
  TSR <- na.omit(TSR)
  #remove trading cost
  for(i in 2:length(TSR)){
    if(TSR$stockID[i]!=TSR$stockID[i-1]){
      TSR$periodrtn[i] <- TSR$periodrtn[i]-0.002
    }
  }

  #get bench mark return
  bench <- getIndexQuote('EI801780',begT = begDate,endT = max(TSR$date),variables = c('pct_chg'),datasrc = 'jy')
  bench <- bench[,c("date","pct_chg")]
  colnames(bench) <- c('date','indexRtn')

  rtn <- merge.x(TSR[,c('date','periodrtn')],bench)
  colnames(rtn) <- c("date","indexRtn","bankRtn")
  rtn <- na.omit(rtn)
  rtn <- xts::xts(rtn[,-1],order.by = rtn[,1])

  tmp <- max(bankPort$date)
  TSF <- TSF[TSF$date==tmp,]
  TSF <- plyr::arrange(TSF,factorscore)
  TSF$mark <- c('')
  tmp <- bankPort[bankPort$date==tmp,'stockID']
  TSF[TSF$stockID==tmp,'mark'] <- 'hold'
  return(list(newData=TSF,bankrtn=rtn))
}


#' connect tinysoft database
#'
#'
#' @author Andrew Dow
#' @return a tinysoft conn.
#' @examples
#' qr <- "setsysparam(pn_stock(),'SZ000002');
#' setsysparam(pn_date(), today()); return nday(30,'date'
#' ,datetimetostr(sp_time()), 'open',open(), 'close',close());"
#' re <- sqlQuery(db.ts(), qr);
#' @export
db.ts <- function(){
  odbcConnect("tinysoftdb")
}



#' ladder position port function
#'
#' @param assetRtn a data frame for stock and bond return.
#' @param ruledf a data frame for position rule.
#' @param rebalance rebalance frequency, default value is NULL.
#' @return a data frame, containing return,postion,nav.
#' @author han.qian
#' @export
#' @examples
#' assetRtn <- rtndemo
#' ruledf <- data.frame(nav = c(0,1.05,1.10,1.20),pos = c(0.15,0.25,0.35,0.5))
#' re <- ladderNAV(assetRtn,ruledf)
#' rebalance <- '2 years'
#' re <- ladderNAV(assetRtn,ruledf,rebalance)
ladderNAV <- function(assetRtn,ruledf,rebalance=NULL){
  assetRtn[,c('pos','rtn','nav_rebalance','nav')] <- 0

  if(is.null(rebalance)){
    for(i in 1:nrow(assetRtn)){
      if(i==1){
        assetRtn$pos[i] <- ruledf$pos[1]
      }else{
        postmp <- findInterval(assetRtn$nav_rebalance[i-1],ruledf$nav)
        assetRtn$pos[i] <- ruledf$pos[postmp]
      }
      assetRtn$rtn[i] <- assetRtn$stock[i]*assetRtn$pos[i]+assetRtn$bond[i]*(1-assetRtn$pos[i])
      assetRtn$nav_rebalance[i] <- prod(1+assetRtn$rtn[1:i])
      assetRtn$nav[i] <- prod(1+assetRtn$rtn[1:i])
    }
  }else{
    reDate <- seq.Date(min(assetRtn$date),max(assetRtn$date),by = rebalance)
    reDate <- QDataGet::trday.nearby(reDate,by=0)
    for( i in 1:nrow(assetRtn)){
      if(assetRtn$date[i] %in% reDate){
        assetRtn$pos[i] <- ruledf$pos[1]
        j <- i
      }else{
        postmp <- findInterval(assetRtn$nav_rebalance[i-1],ruledf$nav)
        assetRtn$pos[i] <- ruledf$pos[postmp]
      }
      assetRtn$rtn[i] <- assetRtn$stock[i]*assetRtn$pos[i]+assetRtn$bond[i]*(1-assetRtn$pos[i])
      assetRtn$nav_rebalance[i] <- prod(1+assetRtn$rtn[j:i])
      assetRtn$nav[i] <- prod(1+assetRtn$rtn[1:i])
    }
  }

  if(is.null(rebalance)){
    assetRtn <- assetRtn[,c("date","stock","bond","pos","rtn","nav")]
  }
  return(assetRtn)
}


#' update table QT_IndexTiming
#'
#'
#' @author Andrew Dow
#' @examples
#' lcdb.update.QT_IndexTiming()
#' @export
lcdb.update.QT_IndexTiming<- function(){

  subfun <- function(indexDate){
    tmp <- brkQT(substr(indexDate$indexID,3,8))

    #get index component
    qr <- paste("select 'EI'+s1.SecuCode 'indexID','EQ'+s2.SecuCode 'stockID',
                convert(varchar(8),l.InDate,112) 'InDate',
                convert(varchar(8),l.OutDate,112) 'OutDate'
                from LC_IndexComponent l
                LEFT join SecuMain s1 on l.IndexInnerCode=s1.InnerCode
                LEFT join SecuMain s2 on l.SecuInnerCode=s2.InnerCode
                where s1.SecuCode in",tmp,
                " order by s1.SecuCode,l.InDate")
    con <- db.jy()
    indexComp <- sqlQuery(con,qr)
    odbcClose(con)
    con <- db.local()
    dbWriteTable(con, name="amtao_tmp", value=indexComp, row.names = FALSE, overwrite = TRUE)
    dbDisconnect(con)

    for(i in 1:nrow(indexDate)){
      cat(i,':',as.character(indexDate$indexID[i]),rdate2int(indexDate$begT[i]),'\n')

      tmpdate <- rdate2int(getRebDates(indexDate$begT[i],indexDate$endT[i],'day'))
      tmpdate <- data.frame(date=tmpdate)
      con <- db.local()
      dbWriteTable(con, name="yrf_tmp", value=tmpdate, row.names = FALSE, overwrite = TRUE)
      dbDisconnect(con)
      qr <- paste("SELECT a.date as date, b.stockID from yrf_tmp a, amtao_tmp b
                  where b.IndexID=", QT(indexDate$indexID[i]),
                  "and b.InDate<=a.date and (b.OutDate>a.date or b.OutDate IS NULL)")
      TS <- dbGetQuery(db.local(),qr)
      TS$date <- intdate2r(TS$date)
      if(i==1){
        tmp <- getRebDates(min(indexDate$begT),max(indexDate$endT),'day')
        tmp <- getTS(tmp,indexID = 'EI801003')
        alldata <- gf.PE_ttm(tmp)
        alldata <- dplyr::rename(alldata,pettm=factorscore)
        tmp <- gf.PB_mrq(tmp)
        tmp <- dplyr::rename(tmp,pbmrq=factorscore)
        alldata <- merge(alldata,tmp,by=c('date','stockID'))
      }

      TSF <- merge.x(TS,alldata,by=c('date','stockID'))
      TSF <- dplyr::filter(TSF,!is.na(pettm),!is.na(pbmrq))

      #pe median
      indexvalue <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pettm))
      indexvalue <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],indexvalue,valtype='PE',caltype='median')

      #pe mean
      tmp <- TSF[,c('date','stockID','pettm')]
      tmp <- dplyr::filter(tmp,pettm>0,pettm<1000)
      colnames(tmp) <- c('date','stockID','factorscore')
      tmp <- RFactorModel:::factor.outlier(tmp,3)
      tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
      tmp <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],tmp,valtype='PE',caltype='mean')
      indexvalue <- rbind(indexvalue,tmp)

      #pb median
      tmp <- plyr::ddply(TSF,'date',plyr::summarise,value=median(pbmrq))
      tmp <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],tmp,valtype='PB',caltype='median')
      indexvalue <- rbind(indexvalue,tmp)

      #pe mean
      tmp <- TSF[,c('date','stockID','pbmrq')]
      tmp <- tmp[tmp$pbmrq>0,]
      colnames(tmp) <- c('date','stockID','factorscore')
      tmp <- RFactorModel:::factor.outlier(tmp,3)
      tmp <- plyr::ddply(tmp,'date',plyr::summarise,value=mean(factorscore))
      tmp <- cbind(indexID=indexDate$indexID[i],indexName=indexDate$indexName[i],tmp,valtype='PB',caltype='mean')
      indexvalue <- rbind(indexvalue,tmp)

      if(i==1){
        re <- indexvalue
      }else{
        re <- rbind(re,indexvalue)
      }
    }
    return(re)
  }#subfun finished

  con <- db.local()
  begT <- dbGetQuery(con,"select max(date) 'date' from QT_IndexTiming")
  begT <- trday.nearby(intdate2r(begT$date),by=-1)
  endT <- trday.nearby(Sys.Date(),by=1)
  if(begT>endT){
    return('Done!')
  }else{
    indexDate <- dbGetQuery(con,"select distinct indexID,indexName from QT_IndexTiming")
    indexDate$begT <- begT
    indexDate$endT <- endT

    re <- subfun(indexDate)
    re$date <- rdate2int(re$date)
    dbWriteTable(con,'QT_IndexTiming',re,overwrite=F,append=T,row.names=FALSE)
    dbDisconnect(con)
    return('Done!')
  }
}





#' calculate fund's tracking error
#'
#' @param fundID is fund ID.
#' @param begT is begin date.
#' @param endT is end date, default value is \bold{today}.
#' @param  scale is number of periods in a year,default value is 250.
#' @examples
#' library(WindR)
#' w.start(showmenu = F)
#' te <- fundTE(fundID='162411.OF',begT=as.Date('2013-06-28'))
#' te <- fundTE(fundID='501018.OF',begT=as.Date('2016-06-28'))
#' @export
fundTE <- function(fundID,begT,endT=Sys.Date(),scale=250){
  if(missing(begT)){
    begT<-w.wss(fundID,'fund_setupdate')[[2]]
    begT <- w.asDateTime(begT$FUND_SETUPDATE,asdate = T)
  }

  fundts <-w.wsd(fundID,"NAV_adj_return1",begT,endT,"Fill=Previous")[[2]]
  tmp <- w.wss(fundID,'fund_benchindexcode')[[2]]
  tmp <- tmp$FUND_BENCHINDEXCODE
  benchts <-w.wsd(tmp,"pct_chg",begT,endT,"Fill=Previous")[[2]]
  allts <- merge(fundts,benchts,by='DATETIME')
  allts <- na.omit(allts)
  allts <- transform(allts,NAV_ADJ_RETURN1=NAV_ADJ_RETURN1/100,PCT_CHG=PCT_CHG/100)
  allts <- xts::xts(allts[,-1],order.by = allts[,1])
  re <- round(PerformanceAnalytics::TrackingError(allts[,1],allts[,2],scale = scale),digits = 3)
  return(re)
}



#' risk parity
#'
#' @param fundID is fund ID.
#' @param begT is begin date.
#' @param endT is end date, default value is \bold{today}.
#' @param  scale is number of periods in a year,default value is 250.
#' @examples
#' suppressMessages(library(PortfolioAnalytics))
#' suppressMessages(library(foreach))
#' suppressMessages(library(iterators))
#' suppressMessages(library(ROI))
#' suppressMessages(library(ROI.plugin.quadprog))
#' suppressMessages(library(ROI.plugin.glpk))
#' asset <- rtndemo
#' asset <- xts::xts(asset[,-1],order.by = asset[,1])
#'
#'
#' @export
risk.parity <- function(asset,rebFreq = "month",training=250,riskcontri){
  funds <- colnames(asset)
  portf <- portfolio.spec(funds)
  portf <- add.constraint(portf, type="full_investment")
  portf <- add.constraint(portf, type="long_only")
  portf <- add.objective(portf, type="return", name="mean")
  portf <- add.objective(portf, type="risk_budget", name="ETL",
     arguments=list(p=0.95), max_prisk=1/3, min_prisk=1/3)

  # Quarterly rebalancing with 5 year training period
  opt_maxret <- optimize.portfolio(R=asset, portfolio=portf,
                                    optimize_method="ROI",
                                    trace=TRUE)

  # Monthly rebalancing with 5 year training period and 4 year rolling window
  bt.opt2 <- optimize.portfolio.rebalancing(asset, portf,
                                            optimize_method="ROI",
                                            rebalance_on="months",
                                            training_period=12,
                                            rolling_window=12)

  ## End(Not run)


}





#' lcdb.update.CorpStockPool
#'
#' @param filenames a vector of filename with path.
#' @examples
#' filenames <- c('D:/sqlitedb/core.csv','D:/sqlitedb/preclose.csv')
#' lcdb.update.CorpStockPool(filenames)
#' @export
lcdb.update.CorpStockPool <- function(filenames){
  all <- data.frame()
  for(i in 1:length(filenames)){
    tmp <- read.csv(filenames[i])
    all <- rbind(all,tmp)
  }
  colnames(all) <- c('stockID','stockName',"MiscellaneousItem",'SecuMarket','FundBelong','CorpStockPool',
                     'InvestAdviceNum','DimensionID','DimensionName','Remark','Operator','AddDate','AddTime',
                     'CheckOperator','HavePosition','ValidBeginDate','ValidEndDate','SecurityCate')
  all$stockID <- stringr::str_pad(all$stockID,6,pad = '0')
  all$stockID <- stringr::str_c('EQ',all$stockID,sep = '')
  con <- db.local()
  dbWriteTable(con,'CT_CorpStockPool',all,overwrite=T,row.names=F)
  dbDisconnect(con)
}



