buildSMB <- function(){
  RebDates <- getRebDates(as.Date('2005-01-31'),as.Date('2016-06-30'),'month')
  TS <- getTS(RebDates,'EI000985')
  TSF <- gf_lcfs(TS,'F000002')
  TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
  TSF <- plyr::ddply(TSF, ~ date, mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
  tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
  tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, mutate, recnum = seq(1,length(date)))
  tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
  tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, mutate, recnum = seq(1,length(date)))

  tmp <- plyr::ddply(tmp.TS1,~date,summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS1 <- plyr::ddply(TS1, ~ datenew, mutate, recnum = seq(1,length(datenew)))
  TS1 <- merge.x(TS1,tmp.TS1)
  TS1 <- TS1[,c("datenew","stockID")]
  colnames(TS1) <- c("date","stockID")

  tmp <- plyr::ddply(tmp.TS3,~date,summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS3 <- plyr::ddply(TS3, ~ datenew, mutate, recnum = seq(1,length(datenew)))
  TS3 <- merge.x(TS3,tmp.TS3)
  TS3 <- TS3[,c("datenew","stockID")]
  colnames(TS3) <- c("date","stockID")

  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(min(TS$date)),
              " and t.TradingDay<=",rdate2int(max(TS$date)))
  con <- db.quant()
  re <- sqlQuery(con,qr)
  close(con)
  re$date <- intdate2r(re$date)

  TSR1 <- merge.x(TS1,re)
  TSR3 <- merge.x(TS3,re)
  R1 <- TSR1[,c("date","stockRtn")]
  R1 <- R1[!is.na(R1$stockRtn),]
  R1 <- plyr::ddply(R1,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))
  R3 <- TSR3[,c("date","stockRtn")]
  R3 <- R3[!is.na(R3$stockRtn),]
  R3 <- plyr::ddply(R3,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))

  rtn <- merge(R1,R3,by='date')
  colnames(rtn) <- c('date','S','B')
  rtn$stockID <- c('EI000985')
  rtn$factorName <- c('SMB')
  rtn$factorScore <- rtn$S-rtn$B
  rtn$date <- rdate2int(rtn$date)
  rtn <- rtn[,c('date','stockID','factorName','factorScore')]

  con <- db.local()
  if(dbExistsTable(con,'QT_FactorScore_amtao')){
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
  }else{
    dbWriteTable(con,'QT_FactorScore_amtao',rtn)
  }
  dbDisconnect(con)
  return('Done!')
}



buildHML <- function(){
  RebDates <- getRebDates(as.Date('2005-01-31'),as.Date('2016-06-30'),'month')
  TS <- getTS(RebDates,'EI000985')
  TSF <- gf_lcfs(TS,'F000006')
  TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
  TSF <- plyr::ddply(TSF, ~ date, mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
  tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
  tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, mutate, recnum = seq(1,length(date)))
  tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
  tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, mutate, recnum = seq(1,length(date)))

  tmp <- plyr::ddply(tmp.TS1,~date,summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS1 <- plyr::ddply(TS1, ~ datenew, mutate, recnum = seq(1,length(datenew)))
  TS1 <- merge.x(TS1,tmp.TS1)
  TS1 <- TS1[,c("datenew","stockID")]
  colnames(TS1) <- c("date","stockID")

  tmp <- plyr::ddply(tmp.TS3,~date,summarise,nstock=length(date))
  tmp.date <- data.frame(date=getRebDates(min(TS$date),max(TS$date),rebFreq = 'day'))
  tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
  tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
  TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                    date=rep(tmp.date$datecor,tmp.date$nstock))
  TS3 <- plyr::ddply(TS3, ~ datenew, mutate, recnum = seq(1,length(datenew)))
  TS3 <- merge.x(TS3,tmp.TS3)
  TS3 <- TS3[,c("datenew","stockID")]
  colnames(TS3) <- c("date","stockID")

  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(min(TS$date)),
              " and t.TradingDay<=",rdate2int(max(TS$date)))
  con <- db.quant()
  re <- sqlQuery(con,qr)
  close(con)
  re$date <- intdate2r(re$date)

  TSR1 <- merge.x(TS1,re)
  TSR3 <- merge.x(TS3,re)
  R1 <- TSR1[,c("date","stockRtn")]
  R1 <- R1[!is.na(R1$stockRtn),]
  R1 <- plyr::ddply(R1,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))
  R3 <- TSR3[,c("date","stockRtn")]
  R3 <- R3[!is.na(R3$stockRtn),]
  R3 <- plyr::ddply(R3,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))

  rtn <- merge(R1,R3,by='date')
  colnames(rtn) <- c('date','H','L')
  rtn$stockID <- c('EI000985')
  rtn$factorName <- c('HML')
  rtn$factorScore <- rtn$H-rtn$L
  rtn$date <- rdate2int(rtn$date)
  rtn <- rtn[,c('date','stockID','factorName','factorScore')]


  con <- db.local()
  if(dbExistsTable(con,'QT_FactorScore_amtao')){
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
  }else{
    dbWriteTable(con,'QT_FactorScore_amtao',rtn)
  }
  dbDisconnect(con)
  return('Done!')
}



lcdb.update.FF3 <- function(){

  update.SMB <- function(){
    con <- db.local()
    begT <- dbGetQuery(con,"select max(date) 'endDate' from QT_FactorScore_amtao where factorName='SMB'")[[1]]
    dbDisconnect(con)
    begT <- trday.nearby(intdate2r(begT),by = -1)
    endT <- Sys.Date()-1
    if(begT>=endT){
      return()
    }
    tmp.begT <- begT - days(day(begT))
    tmp.endT <- endT - days(day(endT))

    RebDates <- getRebDates(tmp.begT,tmp.endT,'month')
    TS <- getTS(RebDates,'EI000985')
    TSF <- gf_lcfs(TS,'F000002')
    TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
    TSF <- plyr::ddply(TSF, ~ date, mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
    tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
    tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, mutate, recnum = seq(1,length(date)))
    tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
    tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, mutate, recnum = seq(1,length(date)))

    tmp <- ddply(tmp.TS1,~date,summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS1 <- plyr::ddply(TS1, ~ datenew, mutate, recnum = seq(1,length(datenew)))
    TS1 <- merge.x(TS1,tmp.TS1)
    TS1 <- TS1[,c("datenew","stockID")]
    colnames(TS1) <- c("date","stockID")

    tmp <- plyr::ddply(tmp.TS3,~date,summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS3 <- plyr::ddply(TS3, ~ datenew, mutate, recnum = seq(1,length(datenew)))
    TS3 <- merge.x(TS3,tmp.TS3)
    TS3 <- TS3[,c("datenew","stockID")]
    colnames(TS3) <- c("date","stockID")

    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    con <- db.quant()
    re <- sqlQuery(con,qr)
    close(con)
    re$date <- intdate2r(re$date)

    TSR1 <- merge.x(TS1,re)
    TSR3 <- merge.x(TS3,re)
    R1 <- TSR1[,c("date","stockRtn")]
    R1 <- R1[!is.na(R1$stockRtn),]
    R1 <- plyr::ddply(R1,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))
    R3 <- TSR3[,c("date","stockRtn")]
    R3 <- R3[!is.na(R3$stockRtn),]
    R3 <- plyr::ddply(R3,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))

    rtn <- merge(R1,R3,by='date')
    colnames(rtn) <- c('date','S','B')
    rtn$stockID <- c('EI000985')
    rtn$factorName <- c('SMB')
    rtn$factorScore <- rtn$S-rtn$B
    rtn$date <- rdate2int(rtn$date)
    rtn <- rtn[,c('date','stockID','factorName','factorScore')]

    con <- db.local()
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
    dbDisconnect(con)
  }

  update.HML <- function(){
    con <- db.local()
    begT <- dbGetQuery(con,"select max(date) 'endDate' from QT_FactorScore_amtao where factorName='HML'")[[1]]
    dbDisconnect(con)
    begT <- trday.nearby(intdate2r(begT),by = -1)
    endT <- Sys.Date()-1
    if(begT>=endT){
      return()
    }
    tmp.begT <- begT - days(day(begT))
    tmp.endT <- endT - days(day(endT))

    RebDates <- getRebDates(tmp.begT,tmp.endT,'month')
    TS <- getTS(RebDates,'EI000985')
    TSF <- gf_lcfs(TS,'F000006')
    TSF <- plyr::ddply(TSF,"date",transform,factorscore=ifelse(is.na(factorscore),median(factorscore,na.rm=TRUE),factorscore))
    TSF <- plyr::ddply(TSF, ~ date, mutate, group = as.numeric(ggplot2::cut_number(factorscore,3)))
    tmp.TS1 <- TSF[TSF$group==1,c("date","stockID")]
    tmp.TS1 <- plyr::ddply(tmp.TS1, ~ date, mutate, recnum = seq(1,length(date)))
    tmp.TS3 <- TSF[TSF$group==3,c("date","stockID")]
    tmp.TS3 <- plyr::ddply(tmp.TS3, ~ date, mutate, recnum = seq(1,length(date)))

    tmp <- plyr::ddply(tmp.TS1,~date,summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS1 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS1 <- plyr::ddply(TS1, ~ datenew, mutate, recnum = seq(1,length(datenew)))
    TS1 <- merge.x(TS1,tmp.TS1)
    TS1 <- TS1[,c("datenew","stockID")]
    colnames(TS1) <- c("date","stockID")

    tmp <- plyr::ddply(tmp.TS3,~date,summarise,nstock=length(date))
    tmp.date <- data.frame(date=getRebDates(begT,endT,rebFreq = 'day'))
    tmp.date$nstock <- tmp$nstock[findInterval(tmp.date$date,tmp$date)]
    tmp.date$datecor <- tmp$date[findInterval(tmp.date$date,tmp$date)]
    TS3 <- data.frame(datenew=rep(tmp.date$date,tmp.date$nstock),
                      date=rep(tmp.date$datecor,tmp.date$nstock))
    TS3 <- plyr::ddply(TS3, ~ datenew, mutate, recnum = seq(1,length(datenew)))
    TS3 <- merge.x(TS3,tmp.TS3)
    TS3 <- TS3[,c("datenew","stockID")]
    colnames(TS3) <- c("date","stockID")

    qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
                from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
                " and t.TradingDay<=",rdate2int(endT))
    con <- db.quant()
    re <- sqlQuery(con,qr)
    close(con)
    re$date <- intdate2r(re$date)

    TSR1 <- merge.x(TS1,re)
    TSR3 <- merge.x(TS3,re)
    R1 <- TSR1[,c("date","stockRtn")]
    R1 <- R1[!is.na(R1$stockRtn),]
    R1 <- plyr::ddply(R1,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))
    R3 <- TSR3[,c("date","stockRtn")]
    R3 <- R3[!is.na(R3$stockRtn),]
    R3 <- plyr::ddply(R3,~date,summarise,dailtRtn=mean(stockRtn,na.rm = T))

    rtn <- merge(R1,R3,by='date')
    colnames(rtn) <- c('date','H','L')
    rtn$stockID <- c('EI000985')
    rtn$factorName <- c('HML')
    rtn$factorScore <- rtn$H-rtn$L
    rtn$date <- rdate2int(rtn$date)
    rtn <- rtn[,c('date','stockID','factorName','factorScore')]


    con <- db.local()
    dbWriteTable(con,'QT_FactorScore_amtao',rtn,overwrite=F,append=T)
    dbDisconnect(con)
  }
  update.SMB()
  update.HML()
  return('Done!')
}


