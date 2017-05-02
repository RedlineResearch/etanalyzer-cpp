#=====================================================================
# Plot the ISMM 2017 data
#    - Raoul Veroy
#=====================================================================
library(ggplot2)
library(reshape2)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
targetfile.pdf <- cargs[2]
targetfile.png <- cargs[3]
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

# sizes <- scan( datafile )
sizes <- read.table( datafile, header = FALSE )

print("===[ 1: SIZES ]========================================================")
length(sizes)
str(sizes)
#--------------------------------------------------
print("======================================================================")
print("Histogram.")
flush.console()
d <- sizes
# p <-qplot( byNodes, data = sizes, xlab = xlabel, main = title, geom = "histogram", binwidth = 1 )
result = tryCatch( {
        p <- ggplot( data = sizes, aes(V1) ) + geom_histogram()
        p <- p + labs( x = "Group size", y = "Count" )
        ggsave(filename = targetfile.pdf, plot = p)
        ggsave(filename = targetfile.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed histogram."))
    }, error = function(e) {
        print(paste("ERROR: failed histogram."))
    }, finally = {
    }
)

print("======================================================================")
print("    DONE.")
flush.console()
