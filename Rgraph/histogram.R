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
    print(paste("Processing", b))
    flush.console()
    subset <- xcsv[ xcsv$benchmark == b, ]
    #--------------------------------------------------
    # Linear scale
    print("    - linear")
    flush.console()
    output <- paste0(b, "-", outfile)
    result = tryCatch( {
            p <- ggplot( subset, aes(x = total)) + geom_histogram( binwidth = 2 )
            ggsave(filename = output, plot = p)
        }, warning = function(w) {
            print(paste("WARNING: failed on", b))
        }, error = function(e) {
            print(paste("ERROR: failed on", b))
        }, finally = {
        }
    )
    #--------------------------------------------------
    # Log scale
    print("    - log")
    flush.console()
    output <- paste0(b, "-log10-", outfile)
    result = tryCatch( {
            p <- ggplot( subset, aes(x = total)) + geom_density() # + scale_y_log10()
            ggsave(filename = output, plot = p)
        }, warning = function(w) {
            print(paste("WARNING: failed on", b))
        }, error = function(e) {
            print(paste("ERROR: failed on", b))
        }, finally = {
        }
    )
    print("    DONE.")
    flush.console()
}
# png(filename = outfile, width = width, height = height)
# qplot( byNodes, data = xcsv, xlab = xlabel, main = title, geom = "histogram", binwidth = 1 )
# dev.off()
