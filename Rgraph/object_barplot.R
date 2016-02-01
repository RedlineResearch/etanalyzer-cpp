#=====================================================================
# Plot the ISMM 2016 data
#    - Raoul Veroy
#=====================================================================
library(ggplot2)
library(lattice)
library(reshape2)

# cargs <- commandArgs(TRUE)
datafile <- "/data/rveroy/pulsrc/data-ismm-2016/all.csv"
objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/object_barplot.pdf"
title <- "Object Count"
xlabel <- "Number of objects allocated"
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add percentage columns
xcsv$byHeap_percent <- round( (xcsv$died_by_heap / xcsv$total_objects) * 100, digits = 1 )
xcsv$byStack_percent <- round( (xcsv$died_by_stack / xcsv$total_objects) * 100, digits = 1 )
# sorted.data <- xcsv

#--------------------------------------------------
# Object count linear scale
print("Object count barplot - linear")
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv, aes(x = benchmark, y = total_objects)) + geom_bar( stat = "identity" ) + coord_flip()
        ggsave(filename = objcount.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on object count - linear scale"))
    }, error = function(e) {
        print(paste("ERROR: failed on object count - linear scale"))
    }, finally = {
    }
)

#--------------------------------------------------
# Object count log scale
print("Object count barplot - log")
objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/object_barplot-log.pdf"
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv, aes(x = benchmark, y = total_objects)) + geom_bar( stat = "identity" ) + coord_flip() + scale_y_log10()
        ggsave(filename = objcount.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on object count - log scale"))
    }, error = function(e) {
        print(paste("ERROR: failed on object count - log scale"))
    }, finally = {
    }
)

#--------------------------------------------------
# Died by heap vs stack percentage  stacked plot
print("Death by stack vs by heap")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/deathcause.pdf"
flush.console()
# mtcars3$car <-factor(mtcars2$car, levels=mtcars2[order(mtcars$mpg), "car"])
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$byHeap_percent), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeap_percent", "byStack_percent")])
xcsv.melt
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip()
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

#--------------------------------------------------
print("    DONE.")
# DEBUG: xcsv.melt
flush.console()
