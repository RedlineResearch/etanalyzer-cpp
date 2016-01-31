#=====================================================================
#
#=====================================================================
library(ggplot2)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
outfile <- cargs[2]
bmark <- cargs[3]
title <- cargs[4]
xlabel <- cargs[5]
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )

print(paste("Processing", bmark))
flush.console()
#--------------------------------------------------
# Types - linear scale
print("    - linear")
flush.console()
output <- paste0(bmark, "-", outfile)
xcsv$num_types <- factor(xcsv$num_types)
result = tryCatch( {
        p <- ggplot( xcsv, aes(x = num_types)) + geom_bar( stat = "bin" )
        ggsave(filename = output, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on", bmark))
    }, error = function(e) {
        print(paste("ERROR: failed on", bmark))
    }, finally = {
    }
)
#--------------------------------------------------
# Types - log scale
print("    - log")
flush.console()
output <- paste0(bmark, "-log10-", outfile)
result = tryCatch( {
        p <- ggplot( xcsv, aes(x = num_types)) + geom_bar( stat = "bin" ) + scale_y_log10()
        ggsave(filename = output, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on", bmark))
    }, error = function(e) {
        print(paste("ERROR: failed on", bmark))
    }, finally = {
    }
)
print("    DONE.")
flush.console()

# png(filename = outfile, width = width, height = height)
# qplot( byNodes, data = xcsv, xlab = xlabel, main = title, geom = "histogram", binwidth = 1 )
# dev.off()
