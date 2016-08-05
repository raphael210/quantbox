

#' add a index to local database
#'
#'
#' @author Andrew Dow
#' @param indexID is index code,such as EI000300
#' @return nothing
#' @examples
#' add.index.lcdb(indexID="EI801003")
#' add.index.lcdb(indexID="EI000985")
#' @export
add.index.lcdb <- function(indexID){
  #check whether the index in local db
  if(indexID=='EI000985'){
    con <- db.local()
    qr <- paste("select * from SecuMain where ID="
                ,QT(indexID),sep="")
    re <- dbGetQuery(con,qr)
    dbDisconnect(con)
    if(nrow(re)>0) return("Already in local database!")

    #part 1 update local SecuMain
    con <- db.jy()
    qr <- paste("select ID,InnerCode,CompanyCode,SecuCode,SecuAbbr,SecuMarket,
                ListedSector,ListedState,JSID 'UpdateTime',SecuCode 'StockID_TS',
                SecuCategory,ListedDate,SecuCode 'StockID_wind'
                from SecuMain WHERE SecuCode=",
                QT(substr(indexID,3,8)),
                " and SecuCategory=4")
    indexInfo <- sqlQuery(con,qr)
    indexInfo <- transform(indexInfo,ID=indexID,
                           SecuCode='000985',
                           StockID_TS='SH000985',
                           StockID_wind='000985.SH')

    #part 2 update local LC_IndexComponent
    qr <- "SELECT 'EI'+s1.SecuCode 'IndexID','EQ'+s2.SecuCode 'SecuID',
    convert(varchar(8),l.InDate,112) 'InDate',convert(varchar(8),l.OutDate,112) 'OutDate',
    l.Flag,l.XGRQ 'UpdateTime',convert(varchar(8),s2.ListedDate,112) 'IPODate'
    FROM LC_IndexComponent l
    inner join SecuMain s1 on l.IndexInnerCode=s1.InnerCode and s1.SecuCode in('801003','000985')
    LEFT join SecuMain s2 on l.SecuInnerCode=s2.InnerCode
    order by s1.SecuCode,l.InDate"
    re <- sqlQuery(con,qr,stringsAsFactors=F)
    indexComp <- re[re$IndexID=='EI000985',c("IndexID","SecuID","InDate","OutDate","Flag","UpdateTime")]

    tmp <- re[re$IndexID=='EI801003' & re$InDate<20110802,]
    tmp <- tmp[substr(tmp$SecuID,1,3) %in% c('EQ0','EQ3','EQ6'),]
    tmp <- transform(tmp,InDate=intdate2r(InDate),
                           OutDate=intdate2r(OutDate),
                           IPODate=intdate2r(IPODate))
    tmp[(tmp$InDate-tmp$IPODate)<90,'InDate'] <- trday.offset(tmp[(tmp$InDate-tmp$IPODate)<90,'IPODate'],by = months(3))
    tmp <- tmp[tmp$InDate<as.Date('2011-08-02'),]
    tmp <- tmp[,c("IndexID","SecuID","InDate","OutDate","Flag","UpdateTime")]

    qr <- "select 'EQ'+s.SecuCode 'SecuID',st.SpecialTradeType,
    ct.MS,convert(varchar(8),st.SpecialTradeTime,112) 'SpecialTradeTime'
    from jydb.dbo.LC_SpecialTrade st,jydb.dbo.SecuMain s,jydb.dbo.CT_SystemConst ct
    where st.InnerCode=s.InnerCode and SecuCategory=1
    and st.SpecialTradeType=ct.DM and ct.LB=1185 and st.SpecialTradeType in(1,2,5,6)
    order by s.SecuCode"
    st <- sqlQuery(con,qr,stringsAsFactors=F)
    odbcCloseAll()
    st <- st[substr(st$SecuID,1,3) %in% c('EQ0','EQ3','EQ6'),]
    st <- st[st$SpecialTradeTime<20110802,]
    st$InDate <- ifelse(st$SpecialTradeType %in% c(2,6),st$SpecialTradeTime,NA)
    st$OutDate <- ifelse(st$SpecialTradeType %in% c(1,5),st$SpecialTradeTime,NA)
    st$InDate <- intdate2r(st$InDate)
    st$OutDate <- intdate2r(st$OutDate)
    st <- st[,c("SecuID","InDate","OutDate")]

    tmp <- rbind(tmp[,c("SecuID","InDate","OutDate")],st)
    tmp <- reshape2::melt(tmp,id=c('SecuID'))
    tmp <- tmp[!(is.na(tmp$value) & tmp$variable=='InDate'),]
    tmp <- unique(tmp)
    tmp[is.na(tmp$value),'value'] <- as.Date('2100-01-01')
    tmp <- plyr::arrange(tmp,SecuID,value)

    tmp$flag <- c(1)
    for(i in 2: nrow(tmp)){
      if(tmp$SecuID[i]==tmp$SecuID[i-1] && tmp$variable[i]==tmp$variable[i-1] && tmp$variable[i]=='InDate'){
        tmp$flag[i-1] <- 0
      }else if(tmp$SecuID[i]==tmp$SecuID[i-1] && tmp$variable[i]==tmp$variable[i-1] && tmp$variable[i]=='OutDate'){
        tmp$flag[i] <- 0
      }else{
        next
      }
    }
    tmp <- tmp[tmp$flag==1,c("SecuID","variable","value")]
    tmp <- plyr::arrange(tmp,SecuID,value)
    tmp1 <- tmp[tmp$variable=='InDate',]
    tmp2 <- tmp[tmp$variable=='OutDate',]
    tmp <- cbind(tmp1[,c("SecuID","value")],tmp2[,"value"])
    colnames(tmp) <- c("SecuID","InDate","OutDate")
    tmp[tmp$OutDate>as.Date('2011-08-02'),'OutDate'] <- as.Date('2011-08-02')
    tmp$IndexID <- 'EI000985'
    tmp$Flag <- 0
    tmp$UpdateTime <- Sys.time()
    tmp$InDate <- rdate2int(tmp$InDate)
    tmp$OutDate <- rdate2int(tmp$OutDate )
    tmp <- tmp[,c("IndexID","SecuID","InDate","OutDate","Flag","UpdateTime")]

    indexComp <- rbind(indexComp,tmp)
    con <- db.local()
    dbWriteTable(con,"SecuMain",indexInfo,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbWriteTable(con,"LC_IndexComponent",indexComp,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbDisconnect(con)
  }else{
    con <- db.local()
    qr <- paste("select * from SecuMain where ID="
                ,QT(indexID))
    re <- dbGetQuery(con,qr)
    dbDisconnect(con)
    if(nrow(re)>0) return("Already in local database!")

    #part 1 update local SecuMain
    con <- db.jy()
    qr <- paste("select ID,InnerCode,CompanyCode,SecuCode,SecuAbbr,SecuMarket,
                ListedSector,ListedState,JSID 'UpdateTime',SecuCode 'StockID_TS',
                SecuCategory,ListedDate,SecuCode 'StockID_wind'
                from SecuMain WHERE SecuCode=",
                QT(substr(indexID,3,8)),
                " and SecuCategory=4",sep='')
    indexInfo <- sqlQuery(con,qr)
    indexInfo <- transform(indexInfo,ID=indexID,
                     StockID_TS=ifelse(is.na(stockID2stockID(indexID,'local','ts')),substr(indexID,3,8),
                                       stockID2stockID(indexID,'local','ts')),
                     StockID_wind=ifelse(is.na(stockID2stockID(indexID,'local','wind')),substr(indexID,3,8),
                                         stockID2stockID(indexID,'local','wind')))

    #part 2 update local LC_IndexComponent
    qr <- paste("SELECT 'EI'+s1.SecuCode 'IndexID','EQ'+s2.SecuCode 'SecuID',
                convert(varchar(8),l.InDate,112) 'InDate',
                convert(varchar(8),l.OutDate,112) 'OutDate',l.Flag,l.XGRQ 'UpdateTime'
                FROM LC_IndexComponent l inner join SecuMain s1
                on l.IndexInnerCode=s1.InnerCode and s1.SecuCode=",
                QT(substr(indexID,3,8))," LEFT join JYDB.dbo.SecuMain s2
                on l.SecuInnerCode=s2.InnerCode")
    indexComp <- sqlQuery(con,qr)
    odbcCloseAll()

    con <- db.local()
    dbWriteTable(con,"SecuMain",indexInfo,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbWriteTable(con,"LC_IndexComponent",indexComp,overwrite=FALSE,append=TRUE,row.names=FALSE)
    dbDisconnect(con)
  }

  return("Done!")
}




#' fix shenwan new industry rule
#'
#' fix local database's shenwan industry rule's bug and make the rule keep consistent.
#' @author Andrew Dow
#' @return nothing.
#' @examples
#' fix.lcdb.swindustry()
#' @export
fix.lcdb.swindustry <- function(){
  con <- db.local()
  qr <- "select * from LC_ExgIndustry where Standard=33"
  re <- dbGetQuery(con,qr)
  dbDisconnect(con)
  if(nrow(re)>0) return("Already in local database!")

  #get raw data
  con <- db.jy()
  qr <- "SELECT 'EQ'+s.SecuCode 'stockID',l.CompanyCode,l.FirstIndustryCode 'Code1',l.FirstIndustryName 'Name1',
  l.SecondIndustryCode 'Code2',l.SecondIndustryName 'Name2',l.ThirdIndustryCode 'Code3',
  l.ThirdIndustryName 'Name3',convert(varchar, l.InfoPublDate, 112) 'InDate',
  convert(varchar, l.CancelDate, 112) 'OutDate',l.InfoSource,l.Standard,l.Industry,
  l.IfPerformed 'Flag',l.XGRQ 'UpdateTime'
  FROM [JYDB].[dbo].[LC_ExgIndustry] l,JYDB.dbo.SecuMain s
  where l.CompanyCode=s.CompanyCode and s.SecuCategory=1
  and s.SecuMarket in(83,90) and l.Standard in(9,24)"
  re <- sqlQuery(con,qr,stringsAsFactors=F)
  re <- re[substr(re$stockID,1,3) %in% c('EQ6','EQ3','EQ0'),]
  re <- re[ifelse(is.na(re$OutDate),T,re$OutDate!=re$InDate),] # remove indate==outdate wrong data

  sw24use <- re[(re$InDate>20140101) & (re$Standard==24),]
  sw9use <- re[(re$InDate<20140101) & (re$Standard==9),]
  sw24tmp <- re[(re$InDate==20140101) & (re$Standard==24),]
  sw9tmp <- sw9use[is.na(sw9use$OutDate) | sw9use$OutDate>20140101,c("stockID","Code1","Name1","Code2","Name2","Code3","Name3")]
  colnames(sw9tmp) <- c("stockID","OldCode1","OldName1","OldCode2","OldName2","OldCode3","OldName3")
  hashtable <- merge(sw24tmp,sw9tmp,by='stockID',all.x=T)
  hashtable <- hashtable[,c("Code1","Name1","Code2","Name2","Code3","Name3","OldCode1","OldName1","OldCode2","OldName2","OldCode3","OldName3")]
  hashtable <- unique(hashtable)
  hashtable <- plyr::ddply(hashtable,~OldName3,plyr::mutate,n=length(OldName3))
  hashtable <- hashtable[hashtable$n==1,c("Code1","Name1","Code2","Name2","Code3","Name3","OldCode1","OldName1","OldCode2","OldName2","OldCode3","OldName3")]

  sw9use <- plyr::rename(sw9use,replace=c("Code1"="OldCode1",
                                    "Name1"="OldName1",
                                    "Code2"="OldCode2",
                                    "Name2"="OldName2",
                                    "Code3"="OldCode3",
                                    "Name3"="OldName3"))
  sw9use <- merge(sw9use,hashtable,by=c("OldCode1","OldName1",
                                        "OldCode2","OldName2",
                                        "OldCode3","OldName3"),all.x=T)
  sw9use <- sw9use[,c("stockID","CompanyCode","Code1","Name1","Code2","Name2",
                      "Code3","Name3","InDate","OutDate","InfoSource","Standard",
                      "Industry","Flag","UpdateTime","OldCode1","OldName1","OldCode2",
                      "OldName2","OldCode3","OldName3")]
  tmp <- sw9use[is.na(sw9use$Code1),c("stockID","CompanyCode","InDate","OutDate","InfoSource","Standard",
                                      "Industry","Flag","UpdateTime","OldCode1","OldName1","OldCode2",
                                      "OldName2","OldCode3","OldName3")]
  sw9use <- sw9use[!is.na(sw9use$Code1),c("stockID","CompanyCode","Code1","Name1","Code2","Name2",
                                          "Code3","Name3","InDate","OutDate","InfoSource","Standard",
                                          "Industry","Flag","UpdateTime")]

  tmp <- plyr::arrange(tmp,stockID,InDate)
  tmp <-merge(tmp,sw24tmp[,c("stockID","Code1","Name1","Code2","Name2","Code3","Name3")],by='stockID',all.x=T)
  zhcn <- unique(sw24use[sw24use$Code1==510000,'Name1'])
  tmp[is.na(tmp$Code1),c("Name1","Name2","Name3")] <- zhcn
  tmp[is.na(tmp$Code1),"Code1"] <-510000
  tmp[is.na(tmp$Code2),"Code3"] <-510100
  tmp[is.na(tmp$Code3),"Code3"] <-510101
  tmp <- tmp[,c("stockID","CompanyCode","Code1","Name1","Code2","Name2",
                "Code3","Name3","InDate","OutDate","InfoSource","Standard",
                "Industry","Flag","UpdateTime")]
  sw9use <- rbind(sw9use,tmp)

  sw33 <- rbind(sw9use,sw24use)
  sw33$Standard <- 33
  sw33$Code1 <- paste('ES33',sw33$Code1,sep = '')
  sw33$Code2 <- paste('ES33',sw33$Code2,sep = '')
  sw33$Code3 <- paste('ES33',sw33$Code3,sep = '')
  sw33$Code99 <- c(NA)
  sw33$Name99 <- c(NA)
  sw33$Code98 <- c(NA)
  sw33$Name98 <- c(NA)
  sw33 <- plyr::arrange(sw33,stockID,InDate)

  #deal with abnormal condition
  #1 outdate<=indate
  sw33 <- sw33[ifelse(is.na(sw33$OutDate),T,sw33$OutDate>sw33$InDate),]
  #2 one stock has two null outdate
  tmp <- plyr::ddply(sw33,'stockID',plyr::summarise,NANum=sum(is.na(OutDate)))
  tmp <- c(tmp[tmp$NANum>1,'stockID'])
  sw33tmp <- sw33[sw33$stockID %in% tmp,]
  sw33 <- sw33[!(sw33$stockID %in% tmp),]
  if(nrow(sw33tmp)>0){
    for(i in 1:(nrow(sw33tmp)-1)){
      if(sw33tmp$stockID[i]==sw33tmp$stockID[i+1] && is.na(sw33tmp$OutDate[i])) sw33tmp$OutDate[i] <- sw33tmp$InDate[i+1]
    }
  }
  sw33 <- rbind(sw33,sw33tmp)
  sw33 <- plyr::arrange(sw33,stockID,InDate)
  #3 indate[i+1]!=outdate[i]
  sw33$tmpstockID <- c(NA,sw33$stockID[1:(nrow(sw33)-1)])
  sw33$tmpOutDate <- c(NA,sw33$OutDate[1:(nrow(sw33)-1)])
  sw33$InDate <- ifelse(ifelse(is.na(sw33$tmpstockID) | is.na(sw33$tmpOutDate),FALSE,sw33$stockID==sw33$tmpstockID & sw33$InDate!=sw33$tmpOutDate),
                        sw33$tmpOutDate,sw33$InDate)
  sw33 <- subset(sw33,select=-c(tmpstockID,tmpOutDate))
  # 4 duplicate indate
  sw33 <- sw33[ifelse(is.na(sw33$OutDate),T,sw33$OutDate>sw33$InDate),]
  sw33[!is.na(sw33$OutDate) & sw33$Flag==1,'Flag'] <- 2

  # update local database CT_IndustryList
  qr <- "SELECT Standard,Classification 'level','ES33'+IndustryCode 'IndustryID'
  ,IndustryName,SectorCode 'Alias','ES33'+FirstIndustryCode 'Code1'
  ,FirstIndustryName 'Name1','ES33'+SecondIndustryCode 'Code2'
  ,SecondIndustryName 'Name2','ES33'+ThirdIndustryCode 'Code3'
  ,ThirdIndustryName 'Name3',UpdateTime
  FROM CT_IndustryType where Standard=24"
  indCon <- sqlQuery(con,qr,stringsAsFactors=F)
  odbcCloseAll()
  indCon$Standard <- 33
  indCon[is.na(indCon$Name2),'Code2'] <- NA
  indCon[is.na(indCon$Name3),'Code3'] <- NA


  con <- db.local()
  dbWriteTable(con,'LC_ExgIndustry',sw33,overwrite=FALSE,append=TRUE,row.names=FALSE)
  dbWriteTable(con,'CT_IndustryList',indCon,overwrite=FALSE,append=TRUE,row.names=FALSE)
  dbDisconnect(con)
  return('Done!')
}




#' combine funcs table.IC and table.Ngroup.spread
#'
#'
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


#' get log(market_value) from local database
#'
#'
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


#' get liquidity factor
#'
#'
#' @author Andrew Dow
#' @param TS is a TS object.
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2015-01-31'),as.Date('2015-12-31'),'month')
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



#' get beta factor
#'
#'
#' @author Andrew Dow
#' @param TS is a TS object.
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2015-01-31'),as.Date('2015-12-31'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.beta(TS)
#' @export
gf.beta <- function(TS){
  check.TS(TS)

  begT <- trday.nearby(min(TS$date),250)
  endT <- max(TS$date)
  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<=",rdate2int(endT))
  con <- db.quant()
  re <- sqlQuery(con,qr)
  odbcCloseAll()
  re <- re[re$stockID %in% unique(TS$stockID),]
  re <- plyr::arrange(re,stockID,date)


  qr <- paste("SELECT convert(varchar(8),q.[TradingDay],112) 'date',
              q.ClosePrice/q.PrevClosePrice-1 'indexRtn'
              FROM QT_IndexQuote q,SecuMain s
              where q.InnerCode=s.InnerCode AND s.SecuCode='801003'
              and q.TradingDay>=",QT(begT),
              " and q.TradingDay<=",QT(endT))
  con <- db.jy()
  index <- sqlQuery(db.jy(),qr)
  odbcCloseAll()

  re <- merge.x(re,index)
  re <- re[!is.na(re$indexRtn),]
  tmp <- as.data.frame(table(re$stockID))
  tmp <- tmp[tmp$Freq>=250,]
  re <- re[re$stockID %in% tmp$Var1,]
  re <- plyr::arrange(re,stockID,date)

  stocks <- unique(re$stockID)
  pb <- txtProgressBar(style = 3)
  for(j in 1:length(stocks)){
    tmp <- re[re$stockID==stocks[j],]
    beta.tmp <- zoo::rollapply(tmp[,c('indexRtn','stockRtn')], width = 250,
                          function(x) coef(lm(stockRtn ~ indexRtn, data = as.data.frame(x)))[2],
                          by.column = FALSE, align = "right")
    beta.tmp <- data.frame(date=tmp$date[250:nrow(tmp)],
                           stockID=stocks[j],
                           factorscore=beta.tmp)
    if(j==1){
      beta <- beta.tmp
    }else{
      beta <- rbind(beta,beta.tmp)
    }
    setTxtProgressBar(pb, j/length(stocks))
  }
  close(pb)
  beta$date <- intdate2r(beta$date)
  beta <- beta[beta$date %in% unique(TS$date),]
  TSF <- merge.x(TS,beta)

  return(TSF)
}



#' get IVR factor
#'
#'
#' @author Andrew Dow
#' @param TS is a TS object.
#' @return a TSF object
#' @examples
#' RebDates <- getRebDates(as.Date('2015-01-31'),as.Date('2015-12-31'),'month')
#' TS <- getTS(RebDates,'EI000300')
#' TSF <- gf.IVR(TS)
#' @export
gf.IVR <- function(TS){
  check.TS(TS)

  lcdb.update.FF3()
  begT <- trday.nearby(min(TS$date),22)
  endT <- max(TS$date)

  con <- db.local()
  qr <- paste("select date,factorScore 'SMB'
              from QT_FactorScore_amtao where factorName='SMB'
              and date>=",rdate2int(begT)," and date<=",rdate2int(endT))
  SMB <- dbGetQuery(con,qr)
  qr <- paste("select date,factorScore 'HML'
              from QT_FactorScore_amtao where factorName='HML'
              and date>=",rdate2int(begT)," and date<=",rdate2int(endT))
  HML <- dbGetQuery(con,qr)
  dbDisconnect(con)

  FF3 <- merge(SMB,HML,by='date')
  con <- db.jy()
  qr <- paste("SELECT convert(varchar(8),q.[TradingDay],112) 'date',
              q.ClosePrice/q.PrevClosePrice-1 'market'
              FROM QT_IndexQuote q,SecuMain s
              where q.InnerCode=s.InnerCode AND s.SecuCode='801003'
              and q.TradingDay>=",QT(rdate2int(begT))," and q.TradingDay<=",QT(rdate2int(endT)),
              " order by q.TradingDay")
  index <- sqlQuery(con,qr)
  FF3 <- merge(FF3,index,by='date')

  qr <- paste("select t.TradingDay 'date',t.ID 'stockID',t.DailyReturn 'stockRtn'
              from QT_DailyQuote t where t.TradingDay>=",rdate2int(begT),
              " and t.TradingDay<=",rdate2int(endT))
  con <- db.quant()
  stockrtn <- sqlQuery(con,qr,stringsAsFactors=F)
  odbcCloseAll()
  stockrtn <- stockrtn[stockrtn$stockID %in% unique(TS$stockID),]
  stockrtn <- plyr::arrange(stockrtn,stockID,date)

  tmp.stock <- unique(stockrtn$stockID)
  IVR <- data.frame()
  pb <- txtProgressBar(style = 3)
  nwin <- 22
  for(i in 1:length(tmp.stock)){
    tmp.rtn <- stockrtn[stockrtn$stockID==tmp.stock[i],]
    tmp.FF3 <- merge(FF3,tmp.rtn[,c('date','stockRtn')],by='date',all.x=T)
    tmp.FF3 <- tmp.FF3[!is.na(tmp.FF3$stockRtn),]
    if(nrow(tmp.FF3)<nwin) next
    tmp <- zoo::rollapply(tmp.FF3[,c("stockRtn","market","SMB","HML")], width =nwin,
                     function(x){
                       x <- as.data.frame(x)
                       if(sum(x$stockRtn==0)>=5){
                         result <- NaN
                       }else{
                         tmp.lm <- lm(stockRtn~market+SMB+HML, data = x)
                         result <- 1-summary(tmp.lm)$r.squared
                       }
                       return(result)},by.column = FALSE, align = "right")
    IVR.tmp <- data.frame(date=tmp.FF3$date[nwin:nrow(tmp.FF3)],
                          stockID=as.character(tmp.stock[i]),
                          IVRValue=tmp)
    IVR <- rbind(IVR,IVR.tmp)
    setTxtProgressBar(pb, i/length(tmp.stock))
  }
  close(pb)
  IVR <- IVR[!is.nan(IVR$IVRValue),]
  IVR$date <- intdate2r(IVR$date)
  colnames(IVR) <- c('date','stockID','factorscore')

  TSF <- merge.x(TS,IVR,by=c('date','stockID'))
  return(TSF)
}



#' get purified factor's portfolio
#'
#' remove style and industry risky factor,get the pure alpha factor's return
#' @author Andrew Dow
#' @param TSFR is a TSFR objetc,factorscore should be standardized and remove NA values
#' @param riskfactorLists is a list of risky factor,such as market value,beta,etc.
#' @return a list of pure factor portfolio's return and weight.
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
#' purefp <- pure.factor.test(TSFR,riskfactorLists)
#' @export
pure.factor.test <- function(TSFR,riskfactorLists){
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
      portrtn <- data.frame(date=dates[i+1],purefactor= (tmp.f[1,] %*% tmp.r))
      portwgt <- data.frame(date=dates[i],stockID=tmp.tsfr$stockID,wgt=tmp.f[1,])
    }else{
      portrtn <- rbind(portrtn,data.frame(date=dates[i+1],purefactor= (tmp.f[1,] %*% tmp.r)))
      portwgt <- rbind(portwgt,data.frame(date=dates[i],stockID=tmp.tsfr$stockID,wgt=tmp.f[1,]))
    }
  }
  portrtn <- xts::xts(portrtn[,-1],order.by = portrtn[,1])
  colnames(portrtn) <- 'pureFactorPort'
  return(list(portRtn=portrtn,portWgt=portwgt))
}





