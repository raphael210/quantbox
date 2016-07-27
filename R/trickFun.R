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






