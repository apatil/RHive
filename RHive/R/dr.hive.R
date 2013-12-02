# Copyright 2014 NexR
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
    m <- match.call(definition = fun, call = sys.call(which = -1), expand.dots = TRUE)
    m[[1L]] <- as.name(sprintf("%s.hive", as.character(m[[1L]])))

#    print(sys.call(which=-1))
    eval(m, envir = parent.frame(n = 2))
  }, error = function(e) {
    print(e)
  } )
  
#  et <- Sys.time()
#  cat(sprintf("+ processing time: %f", as.numeric(et-st)))
}


envvar <- function(name) {
  value <- Sys.getenv(name)
  if (!is.na(value) && !is.null(value)) {
    v <- as.character(value)
    if (nchar(v) > 0) {
      return (v)
    }
  }

  return (NULL)
}

init.sysenv <- function() {
  hive.home <- envvar("HIVE_HOME")   
  hive.server.version <- envvar("HIVESERVER_VERSION")   
  hive.lib <- envvar("HIVE_LIB_DIR")
  fs.home <- envvar("HDFS_HOME")  

  sys.env <- new("sysenv", hive.home = hive.home, hive.server.version = hive.server.version, hive.lib = hive.lib, fs.home = fs.home)

  assign("sysenv", sys.env, envir = context) 
}

sysenv <- function() {
  get("sysenv", envir = context)
}

info.hive <- function(host = "127.0.0.1", port = 10000L, is.server2 = NA) {
  sysenv <- sysenv()
  if (is.na(is.server2)) {
    is.server2 <- TRUE
    if (!is.null(sysenv)) {
      val <- sysenv@hive.server.version
      if (!is.null(val)) {
        ver <- as.integer(val)
        if (ver == 1L) {
          is.server2 <- FALSE
        }
      }
    } 
  }

  new ("hive.info", host = host, port = port, is.server2 = is.server2)
}


configuration <- function() {
  get("config", envir = context)
}

java.classpath <- function() {

}

init.jvm <- function(java.params = getOption("java.parameters")) {
  cp <- java.classpath()
  .jinit(classpath=cp, parameters = java.params)
}

connect.hive <- function(info, db = "default", user = NULL, password = NULL, auth.properties = character(0)) {
  conf <- configuration()
  init.jvm(conf@java.params)

  props <- j2r.Properties()
  if (!is.null(user) && !is.na(user)) {
    props$setProperty("user", user)
  }

  if (!is.null(password) && !is.na(password)) {
    props$setProperty("password", password)
  }

  if (!is.null(auth.properties) && length(auth.properties)  > 0) {
    l <- lapply(strsplit(auth.properties, split="="), function(x) { gsub("^\\s+|\\s+$", "", x) })
    lapply(l, function(p) { if (length(p) == 2) { props$setProperty(p[1], p[2]) } })
  }

  client <- j2r.HiveJdbcClient(info@is.server2)
  client$connect(info@host, as.integer(info@port), db, props)
  check.jars(client)

  set.udfs(client)
  set.hiveconf(client)

  mk.hdfsdirs(client)

  if (is.null(user)) {
    user <- Sys.info()[["user"]]
  }

  new ("hive.connection", info = info, session = new ("hive.session", pseudo.user = user), client = client)
}

check.jars <- function(client) {

}

set.udfs <- function(client) {

}

set.hiveconf(client) {

}

mk.hdfsdirs(client) {

}





query.hive <- function(connection, query, fetchsize=50, limit=-1) {
  client <- connection@client
  result <- client$query(query, as.integer(limit), as.integer(fetchsize))
  
  process(result) 
}


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
