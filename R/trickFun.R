#' alpha portfolio demo data
#'
#' part of index EI000905's alpha portfolio data set.
#' @format A data frame with 1149 rows and 3 variables.
"portdemo"




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




