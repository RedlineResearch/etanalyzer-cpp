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
xlabel <- cargs[6]
xcsv <- read.table( datafile, sep = ",", header = TRUE )

png(filename = outfile, width = width, height = height)
qplot( byNodes, data = xcsv, xlab = xlabel, main = title, geom = "histogram", binwidth = 1 )
dev.off()
