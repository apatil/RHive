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



setClass ("sys.envir",
	representation (hive.home = "character",
			hive.server.version = "integer"),
	prototype (hive.home = character(0),
		hive.server.version = integer(0)))	


setClass ("config",
	representation (jvm.params = "character",
			hiveconf = "character",
			log.level = "character"),
	prototype (jvm.params = getOption("java.parameters"),
		log.level = "warning"))		


setClass ("service", 
	representation (name = "character",
			obj = "jobjRef"),
	prototype (name = character(0),
		obj = .jnull(class="java/lang/Object")))


setClass ("info", 
	representation (host = "character",
			port = "integer"),
	prototype (host = "127.0.0.1",
		port = integer(1)))


setClass ("hive.info",
	representation (is.server2 = "logical"),
	contains = "info",
	prototype (port = 10000L,
		is.server2 = TRUE))


setClass ("session",
	representation (user = "character",
			wd = "character",
			tempdir = "character"),
	prototype (user = Sys.info()["user"],
		wd = getwd(),
		tempdir = tempdir()))


setClass ("hive.session",
	representation (pseudo.user = "character"),
	contains = "session",
	prototype (new ("session")))


setClass ("connection",
	representation (info = "info",
			session = "session"))


setClass ("hive.connection",
	representation (client = "jobjRef"),
	contains = "connection")


setValidity("hive.connection",
	function(object) {
		ret <- NULL
		if (object@client@jclass != "com/nexr/rhive/hive/HiveJdbcClient") {
			ret <- c(ret, "hive.connection@client must be a instance of com/nexr/rhive/hive/HiveJdbcClient")
		}

		if (is.null(ret)) {
			return (TRUE)
		} else {
			return (ret)
		}
	}
)


setClass ("async.op",
	representation (id = "character",
			op = "character",
			future = "jobjRef"),
	prototype (id = character(0),
		op = character(0),
		future = .jnull(class="java/util/concurrent/Future"))) 



setClass ("data.frame.set",
	representation (id = "character",
			size = "integer",
			names = "character",
			set = "list"),
	prototype (id = character(0),
		size = integer(0),
		names = character(0),
		set = list()))
