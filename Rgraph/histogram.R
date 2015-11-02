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

bmark <- unique(xcsv$benchmark)
for (b in bmark) {
    print(b)
    subset <- xcsv[ xcsv$benchmark == b, ]
    #--------------------------------------------------
    # Linear scale
    output <- paste0(b, "-", outfile)
    p <- ggplot( subset, aes(x = total)) + geom_histogram( binwidth = 2 )
    ggsave(filename = output, plot = p)
    #--------------------------------------------------
    # Log scale
    output <- paste0(b, "-log10-", outfile)
    p <- ggplot( subset, aes(x = total)) + geom_density() # + scale_y_log10()
    ggsave(filename = output, plot = p)
}
# png(filename = outfile, width = width, height = height)
# qplot( byNodes, data = xcsv, xlab = xlabel, main = title, geom = "histogram", binwidth = 1 )
# dev.off()
