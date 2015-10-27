#=====================================================================
#
#=====================================================================
library(ggplot2)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
outfile <- cargs[2]
width <- as.integer(cargs[3])
height <- as.integer(cargs[4])
title <- cargs[5]
xcsv <- read.table( datafile, sep = ",", header=TRUE )
# hist( xcsv$byNodes,
#       breaks = max(xcsv$byNodes),
#       col = "blue",
#       xlab = "Component Node Count",
#       main = "Distribution of Component Node Count" )
png(filename = outfile, width = width, height = height)
qplot( byNodes, data = xcsv, xlab = "Component Node Count", main = title, geom = "histogram", binwidth = 1 )
dev.off()
