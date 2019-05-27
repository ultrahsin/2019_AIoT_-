# 用cdata開發的程式把Aure Storage table SQL化(免費30天)
# https://www.cdata.com/kb/tech/azure-odbc-r.rst
# https://kanchengzxdfgcv.blogspot.com/2016/04/ms-sql-server-rodbc.html

# 5/27定時迴圈
# https://www.ptt.cc/bbs/R_Language/M.1499754775.A.D03.html
# https://www.ptt.cc/bbs/R_Language/M.1453699139.A.54C.html
# https://blog.gtwang.org/r/r-flow-control-and-loops/3/

# SQL大神
# https://www.1keydata.com/tw/sql/sqldelete.html

library(RODBC)
conn <- odbcConnect("CData AzureTables Source")
sqlTables(conn)
DT_project <- sqlQuery(conn, "SELECT * FROM Projects", believeNRows=FALSE, rows_at_time=1)

count <- 0

repeat{
  DT_project <- sqlQuery(conn, "SELECT * FROM Projects", believeNRows=FALSE, rows_at_time=1)
  
  if(DT_project$Value[which(DT_project$PartitionKey == "CheckID")] %in% DT_project$Value[1:(which(DT_project$PartitionKey == "CheckID")-1)]){
    # DT_project$Value[which(DT_project$PartitionKey == "Door")] <- "1"
    update.sql <- paste("UPDATE Projects", "SET Value = 1", "WHERE PartitionKey =", "'Door'", "AND RowKey =", "'002'")
    sqlQuery(conn, update.sql)
    Sys.sleep(5)
    update.sql <- paste("UPDATE Projects", "SET Value = 0", "WHERE PartitionKey =", "'Door'", "AND RowKey =", "'002'")
    sqlQuery(conn, update.sql)
    update.sql <- paste("UPDATE Projects", "SET Value = 0", "WHERE PartitionKey =", "'CheckID'", "AND RowKey =", "'001'")
    sqlQuery(conn, update.sql)
  }else if(DT_project$Value[which(DT_project$PartitionKey == "PB1")] == "1"){
    update.sql <- paste("UPDATE Projects", "SET Value = 1", "WHERE PartitionKey =", "'Door'", "AND RowKey =", "'002'")
    sqlQuery(conn, update.sql)
    Sys.sleep(5)
    update.sql <- paste("UPDATE Projects", "SET Value = 0", "WHERE PartitionKey =", "'Door'", "AND RowKey =", "'002'")
    sqlQuery(conn, update.sql)
    update.sql <- paste("UPDATE Projects", "SET Value = 0", "WHERE PartitionKey =", "'PB1'", "AND RowKey =", "'003'")
    sqlQuery(conn, update.sql)
  }
  Sys.sleep(1)
  count <- count + 1
  if(count == 120) break
}

odbcClose(conn)
