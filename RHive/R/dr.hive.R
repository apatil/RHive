# Copyright 2013 NexR
#    
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


call.internal <- function(fun) {
#  st <- Sys.time()
  tryCatch ( {
    m <- match.call(definition=fun, call=sys.call(which=-1), expand.dots=TRUE)
    m[[1L]] <- as.name(sprintf("%s.hive", as.character(m[[1L]])))

#    print(sys.call(which=-1))
    eval(m, envir=parent.frame(n=2))
  }, error=function(e) {
    print(e)
  } )
  
#  et <- Sys.time()
#  cat(sprintf("+ processing time: %f", as.numeric(et-st)))
}


##
#
# return hive infomation
##
info.hive <- function (host="127.0.0.1", port = 10000, is.server2 = NA) {
  if (is.na(is.server2)) {
    # server2 <- 
  }

  new ("hive.info", host = host, port = port, is.server2 = is.server2)
}


##
# 
# return connection
##
connect.hive <- function(info, db = "default", user = NULL, password = NULL) {
  init.jvm()

  client <- connect(info$host, as.integer(info$port), db, user, password)
  check.jars(client)

  register.udfs(client)
  set.configs(client)

  make.basedirs(client)

  if (is.null(user)) {
    user <- Sys.info()[["user"]] 
  }

  new ("hive.connection", info = info, session = new ("hive.session", pseudo.user = user), client = client)
}

##
#
# return data frame object
##
query.hive <- function(connection, query, fetchsize=50, limit=-1) {
  client <- connection@client
  result <- client$query(query, as.integer(limit), as.integer(fetchsize))
  
  process(result) 
}

##
#
# return 
##
execute.hive <- function(connection, query) {
  client <- connection@client
  client$execute(query)
}

load.hive <- function(connection, table, subset, columns = "*", strings.as.factors = TRUE) {
  if (cols != "*") {
    paste(cols, collapse = ", ")
  }

  
}

connection.properties <- function(host, port, db, user, password) {
  return (java.HiveConnectionProperties(host, port, db, user, password))
}

async.query.hive <- function(id, query, host = "127.0.0.1", port = 10000L, db = "default", user = NLL, password = NULL) {
  executor <- async.executor()
  future <- executor$execute(id, query, connection.properties(host, port, db, user, password))
  
  task <- new("async.task", id = id, op = "query", future = future)
  return (task)
}

async.execute.hive <- function(id, query, host = "127.0.0.1", port = 10000L, db = "default", user = NLL, password = NULL) {
  executor <- async.executor()
  future <- executor$execute(id, query, connection.properties(host, port, db, user, password))
  
  task <- new("async.task", id = id, op = "execute", future = future)
  return (task)
}


##
#
# return 
##
write.hive <- function(connection, data, table, drop.row.names = TRUE, row.names.column = "rownames") {

}

##
#
# return 
##
set.hive <- function(connection, key, value) {

}

##
#
# return 
##
unset.hive <- function(connection, key) {

}

##
#
# return 
##
show.databases.hive <- function(connection) {

}

##
#
# return 
##
use.database.hive <- function(connection, database) {

}

##
#
# return 
##
show.tables.hive <- function(connection) {

}

##
#
# return 
##
desc.table.hive <- function(connection, table, extended=FALSE) {

}

##
#
# return 
##
register.udf.hive <- function(connection) {

}

##
#
# return 
##
register.udaf.hive <- function(connection) {

}

##
#
# return table name
##
mapreduce.hive <- function(connection) {

}

##
#
# return 
##
close.hive <- function(connection) {

}


init.jvm <- function() {

}

check.jars <- function(client) {

}

register.udfs <- function(client) {

}

set.configs <- function(client) {

}

make.basedirs <- function(client) {

}

set.default <- function(key, value) {

}

get.default <- function(key) {

}

dfs.mkdirs <- function() {

}
