

stat.mem.rhel <- function() {
  out <- system("free", intern=TRUE)
  names <- unlist(strsplit(out[1], split="\\s+"))[-1]
  stat <- as.integer(unlist(strsplit(out[2], split="\\s+"))[-1])
  names(stat) <- names
  return (stat)
}

df.rhel <- function() {
  system("df -h")
}


