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



setClass ("dr.info", 
	representation (host = "character",
			port = "integer"),
	prototype (host = "127.0.0.1",
		port = integer(1)))


setClass ("hive.info",
	representation (is.server2 = "logical"),
	contains = "dr.info",
	prototype (port = 10000L,
		is.server2 = TRUE))


setClass ("dr.session",
	representation (user = "character",
			wd = "character",
			tempdir = "character"),
	prototype (user = Sys.info()["user"],
		wd = getwd(),
		tempdir = tempdir()))


setClass ("hive.session",
	representation (pseudo.user = "character"),
	contains = "dr.session",
	prototype (new ("dr.session")))


setClass ("dr.connection",
	representation (info = "dr.info",
			session = "dr.session"))


setClass ("hive.connection",
	representation(client = "jobjRef"),
	contains = "dr.connection")


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


