#=====================================================================
# Plot the ISMM 2016 data
#    - Raoul Veroy
#=====================================================================
library(ggplot2)
library(lattice)
library(reshape2)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
# datafile <- "/data/rveroy/pulsrc/data-ismm-2016/all.csv"
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
        p <- ggplot( xcsv, aes(x = benchmark, y = total_objects)) +
             geom_bar( stat = "identity" ) + coord_flip()
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
        p <- ggplot( xcsv, aes(x = benchmark, y = total_objects)) +
             geom_bar( stat = "identity" ) + coord_flip() + scale_y_log10()
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
# DEBUG:
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#1F78B4", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

print("======================================================================")
#--------------------------------------------------
# Died by heap vs stack size percentage  stacked plot
print("Death by stack vs by heap - size")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/deathcause-size.pdf"
flush.console()
d <- xcsv
d$byHeapSize_percent <- round( (d$died_by_heap_size / (d$died_by_heap_size + d$died_by_stack_size)) * 100, digits = 1 )
d$byStackSize_percent <- round( (d$died_by_stack_size / (d$died_by_heap_size + d$died_by_stack_size)) * 100, digits = 1 )
d$benchmark <- factor(d$benchmark, levels = d[ order(d$byHeapSize_percent), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeapSize_percent", "byStackSize_percent")])
# DEBUG:
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#1F78B4", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

print("======================================================================")
#--------------------------------------------------
# Died by heap vs stack percentage  stacked plot
# with stack split into by 
# - stack only
# - stack after heap
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/deathcause-stack.pdf"
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/deathcause-stack.pdf"
flush.console()
d <- xcsv
d$byStackOnly_percent <- round( (d$died_by_stack_only / d$total_objects) * 100, digits = 1 )
d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$total_objects) * 100, digits = 1 )
d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$total_objects) * 100, digits = 1 )
d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null) / d$total_objects) * 100, digits = 1 )
# d$byHeapAfterValid_percent[ d$byHeapAfterValid_percent < 0 ] <- 0.0
d$benchmark <- factor( d$benchmark, levels = d[ order( d$byHeap_percent), "benchmark" ] )
xcsv.heap.melt <- melt(d[,c( "benchmark", "byHeapAfterNull_percent", "byHeapAfterValid_percent", "byStack_percent" )])
xcsv.stack.melt <- melt(d[,c( "benchmark", "byHeap_percent", "byStackOnly_percent", "byStackAfterHeap_percent" )])
# DEBUG:
# print("======================================================================")
# xcsv.stack.melt
# print("======================================================================")
# xcsv.heap.melt
print("======================================================================")
print("Death by stack broken down")
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#1F78B4", "#B2DF8A", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

print("======================================================================")
print("Death by heap broken down")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/deathcause-heap.pdf"
xcsv.heap.melt
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.heap.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#A6CEE3", "#1F78B4", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

# =====================================================================
d <- xcsv
d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$died_by_heap) * 100, digits = 1 )
d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null_heap) / d$died_by_heap) * 100, digits = 1 )

# d$byStackOnly_percent <- round( (d$died_by_stack_only / d$died_by_stack) * 100, digits = 1 )
# d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$died_by_stack ) * 100, digits = 1 )

d$benchmark <- factor( d$benchmark, levels = d[ order( d$byHeapAfterNull_percent), "benchmark" ] )
xcsv.heap.melt <- melt(d[,c( "benchmark", "byHeapAfterNull_percent", "byHeapAfterValid_percent" )])
print("======================================================================")
print("Death by heap broken down")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/deathcause-heap-only.pdf"
xcsv.heap.melt
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.heap.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#A6CEE3", "#1F78B4") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

print("======================================================================")
print("    DONE.")
flush.console()
