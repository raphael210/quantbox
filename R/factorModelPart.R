#' brkQT
#'
#' bracket a series of string.
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







#' table.factor.summary
#'
#' combine table.IC and table.Ngroup.spread
#' @author Andrew Dow
#' @param TSFR is a TSFR object
#' @param N seprate the stocks into N group
#' @param fee trading cost
#' @return a factor's IC summary and longshort portfolio's return summary.
#' @examples
#' modelPar <- modelPar.default()
#' TSFR <- Model.TSFR(modelPar)
#' table.factor.summary(TSFR)
#' @export
table.factor.summary <- function(TSFR,N=10,fee=0.001){
  seri <- seri.IC(TSFR)
  seri <- as.vector(seri)
  IC.mean <- mean(seri,na.rm=TRUE)
  IC.std <- sd(seri,na.rm=TRUE)
  IC.IR <- IC.mean/IC.std
  IC.hit <- hitRatio(seri)

  ICsum <- c(IC.mean, IC.std, IC.IR, IC.hit)
  ICsum <- matrix(ICsum,length(ICsum),1)
  rownames(ICsum) <- c("IC.mean","IC.std","IC.IR","IC.hitRatio")

  rtnseri <- seri.Ngroup.rtn(TSFR,N=N)
  turnoverseri <- seri.Ngroup.turnover(TSFR,N=N)
  spreadseri <- rtnseri[,1]-rtnseri[,ncol(rtnseri)]
  rtnsummary <- rtn.summary(spreadseri)
  turnover.annu <- Turnover.annualized(turnoverseri)
  turnover.annu <- sum(turnover.annu[,c(1,ncol(turnover.annu))])/2
  rtn.feefree <- rtnsummary[1,]-turnover.annu*fee*2*2   # two side trade and two groups
  rtnsum <- rbind(rtnsummary,turnover.annu,rtn.feefree)
  rownames(rtnsum)[c(nrow(rtnsum)-1,nrow(rtnsum))] <- c("Annualized Turnover","Annualized Return(fee cut)")

  re <- rbind(ICsum,rtnsum)
  colnames(re) <- 'factorSummary'
  re <- round(re,digits = 3)
  return(re)
}


#' gf.ln_mkt_cap
#'
#' get ln(market_value) in local db
#' @author Andrew Dow
#' @param TS is a TS object.
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2011-03-17'),as.Date('2012-04-17'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.ln_mkt_cap(TS)
#' @export
gf.ln_mkt_cap <- function(TS){
  check.TS(TS)
  TSF <- gf_lcfs(TS, 'F000002')
  TSF$factorscore <- ifelse(is.na(TSF$factorscore),NA,log(TSF$factorscore))
  return(TSF)
}


#' gf.liquidity
#'
#' get liquidity factor in local db
#' @author Andrew Dow
#' @param TS is a TS object.
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2010-03-31'),as.Date('2015-12-31'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.liquidity(TS)
#' @export
gf.liquidity <- function(TS){
  check.TS(TS)

  begT <- trday.nearby(min(TS$date),21)
  endT <- max(TS$date)
  conn <- db.quant()
  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.TurnoverVolume,t.NonRestrictedShares
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<=",rdate2int(endT))
  re <- sqlQuery(conn,qr)
  close(conn)
  re <- re[re$stockID %in% c(unique(TS$stockID)),]
  re$TurnoverRate <- abs(re$TurnoverVolume/(re$NonRestrictedShares*10000))
  re <- re[,c("date","stockID","TurnoverRate")]
  tmp <- as.data.frame(table(re$stockID))
  tmp <- tmp[tmp$Freq>=21,]
  re <- re[re$stockID %in% tmp$Var1,]
  re <- plyr::arrange(re,stockID,date)

  re <- plyr::ddply(re,"stockID",plyr::mutate,factorscore=zoo::rollsum(TurnoverRate,21,fill=NA,align = 'right'))
  re <- subset(re,!is.na(re$factorscore))
  re <- subset(re,factorscore>=0.000001)
  re$factorscore <- log(re$factorscore)
  re <- re[,c("date","stockID","factorscore")]
  re$date <- intdate2r(re$date)
  re <- re[re$date %in% c(unique(TS$date)),]

  TSF <- merge.x(TS,re)
  return(TSF)
}




#' pureFactorTest
#'
#' remove style and industry risky factor,get the pure alpha factor's return
#' @author Andrew Dow
#' @param TSFR is a TSFR objetc,factorscore should be standardized and remove NA values
#' @param riskfactorLists is a list of risky factor,such as market value,beta,etc.
#' @return pure factor return
#' @examples
#' begT <- as.Date('2012-01-31')
#' endT <- as.Date('2016-06-30')
#' RebDates <- getRebDates(begT,endT)
#' TS <- getTS(RebDates,'EI000905')
#' TSF <- getTSF(TS,factorFun = 'gf_lcfs',factorPar = list(factorID='F000017'),factorDir = -1,
#'               factorStd = 'norm')
#' TSFR <- getTSR(TSF)
#' riskfactorLists <- buildFactorLists(buildFactorList(factorFun = "gf.ln_mkt_cap",factorDir = -1,
#'                          factorStd = "norm"))
#' factorIDs <- c("F000006","F000015","F000016")
#' tmp <- buildFactorLists_lcfs(factorIDs,factorStd="norm")
#' riskfactorLists <- c(riskfactorLists,tmp)
#' factorRtn <- pureFactorTest(TSFR,riskfactorLists)
#' @export
pureFactorTest <- function(TSFR,riskfactorLists){
  TSFR <- TSFR[!is.na(TSFR$factorscore),]
  new.name <- colnames(TSFR)

  #get risky factor value
  TSFrisk <- getMultiFactor(TSFR[,c('date','stockID')],riskfactorLists)
  new.name <- c(new.name,colnames(TSFrisk)[-1:-2])

  #get sectorID
  TSS <- getSectorID(TSFR[,c('date','stockID')],sectorAttr = list(33,1))
  TSS[is.na(TSS$sector),'sector'] <- 'ES33510000'
  TSS <- reshape2::dcast(TSS,date+stockID~sector,length,fill=0,value.var = 'sector')
  new.name <- c(new.name,colnames(TSS)[-1:-2])

  #get sqrt floating market value
  TSFfv <- getTSF(TSFR[,c('date','stockID')],factorFun = 'gf_lcfs',factorPar = list(factorID='F000001'),
                  factorStd = 'none',factorNA = "median")
  TSFfv$factorscore <- sqrt(TSFfv$factorscore)
  colnames(TSFfv)[3] <- 'fv'
  new.name <- c(new.name,colnames(TSFfv)[-1:-2])

  TSFR <- merge.x(TSFR,TSFrisk,by = c('date','stockID'))
  TSFR <- merge.x(TSFR,TSS,by = c('date','stockID'))
  TSFR <- merge.x(TSFR,TSFfv,by = c('date','stockID'))
  TSFR <- TSFR[,c(new.name)]

  dates <- unique(TSFR$date)
  for(i in 1:(length(dates)-1)){
    tmp.tsfr <- TSFR[TSFR$date==dates[i],]
    tmp.x <- as.matrix(tmp.tsfr[,c(3,6:(ncol(tmp.tsfr)-1))])
    tmp.x <- subset(tmp.x,select = (colnames(tmp.x)[colSums(tmp.x)!=0]))
    tmp.r <- as.matrix(tmp.tsfr[,"periodrtn"])
    tmp.r[is.na(tmp.r)] <- mean(tmp.r,na.rm = T)
    tmp.w <- as.matrix(tmp.tsfr[,"fv"])
    tmp.w <- diag(c(tmp.w),length(tmp.w))
    tmp.f <- solve(crossprod(tmp.x,tmp.w) %*% tmp.x) %*% crossprod(tmp.x,tmp.w)
    if(i==1){
      portrtn=data.frame(date=dates[i+1],purefactor= (tmp.f[1,] %*% tmp.r))
    }else{
      portrtn <- rbind(portrtn,data.frame(date=dates[i+1],purefactor= (tmp.f[1,] %*% tmp.r)))
    }
  }
  portrtn <- xts::xts(portrtn[,-1],order.by = portrtn[,1])
  colnames(portrtn) <- 'pureFactorPort'
  return(portrtn)
}


