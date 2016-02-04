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
objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/01-object_barplot.pdf"
title <- "Object Count"
xlabel <- "Number of objects allocated"
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add derived information
xcsv$byHeap_percent <- round( (xcsv$died_by_heap / xcsv$total_objects) * 100, digits = 1 )
xcsv$byStack_percent <- round( (xcsv$died_by_stack / xcsv$total_objects) * 100, digits = 1 )
xcsv$totalSize <- xcsv$died_by_stack_size + xcsv$died_by_heap_size

print("======================================================================")
xcsv

print("======================================================================")
#--------------------------------------------------
# Object count linear scale
print("Object count barplot - linear")
flush.console()
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = total_objects)) +
             geom_bar( stat = "identity" ) + coord_flip()
        ggsave(filename = objcount.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on object count - linear scale"))
    }, error = function(e) {
        print(paste("ERROR: failed on object count - linear scale"))
    }, finally = {
    }
)

print("======================================================================")
#--------------------------------------------------
# Size
print("Size barplot")
objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/02-size-barplot.pdf"
flush.console()
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = totalSize )) +
             geom_bar( stat = "identity" ) + coord_flip()
        ggsave(filename = objcount.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on size barplot"))
    }, error = function(e) {
        print(paste("ERROR: failed on size barplot"))
    }, finally = {
    }
)

print("======================================================================")
#--------------------------------------------------
# Died by heap vs stack percentage  stacked plot - object count
print("Death by stack vs by heap")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/03-percent-deathcause-object-count.pdf"
flush.console()
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
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

deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/03-actual-deathcause-object-count.pdf"
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "died_by_heap", "died_by_stack")])
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
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/04-percent-deathcause-size.pdf"
flush.console()
d <- xcsv
# Sort the benchmarks
d$byHeapSize_percent <- round( (d$died_by_heap_size / (d$died_by_heap_size + d$died_by_stack_size)) * 100, digits = 1 )
d$byStackSize_percent <- round( (d$died_by_stack_size / (d$died_by_heap_size + d$died_by_stack_size)) * 100, digits = 1 )
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeapSize_percent", "byStackSize_percent")])
d.actual.size.melt <- melt(d[,c("benchmark", "died_by_heap_size", "died_by_stack_size")])
# DEBUG:
# Plot percentage breakdown heap vs stack - size
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

# Plot actual breakdown heap vs stack - size
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/04-actual-deathcause-size.pdf"
result = tryCatch( {
        p <- ggplot( d.actual.size.melt,
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
# Died by heap vs stack percentage  stacked plot - object count
# with stack split into by 
# - stack only
# - stack after heap
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/05-deathcause-stack.pdf"
flush.console()
d <- xcsv
# Sort the benchmarks
d$byStackOnly_percent <- round( (d$died_by_stack_only / d$total_objects) * 100, digits = 1 )
d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$total_objects) * 100, digits = 1 )
d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null_heap) / d$total_objects) * 100, digits = 1 )
d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$total_objects) * 100, digits = 1 )
d$benchmark <- factor( d$benchmark, levels = d[ order( d$max_live_size), "benchmark" ] )
xcsv.heap.melt <- melt(d[,c( "benchmark", "byHeapAfterNull_percent", "byHeapAfterValid_percent", "byStack_percent" )])
xcsv.stack.melt <- melt(d[,c( "benchmark", "byHeap_percent", "byStackOnly_percent", "byStackAfterHeap_percent" )])

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
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/07-deathcause-heap.pdf"
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
# Sort the benchmarks
d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$died_by_heap) * 100, digits = 1 )
d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null_heap) / d$died_by_heap) * 100, digits = 1 )
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.heap.melt <- melt(d[,c( "benchmark", "byHeapAfterNull_percent", "byHeapAfterValid_percent" )])
xcsv.heap.melt
print("======================================================================")
print("Death by heap broken down")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/08-deathcause-heap-only.pdf"
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.heap.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#A6CEE3", "#1F78B4") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on heap last null breakdown."))
    }, error = function(e) {
        print(paste("ERROR: failed on heap last null breakdown."))
    }, finally = {
    }
)

print("======================================================================")
d <- xcsv
# Sort the benchmarks
# Drill down to stack only - by stack only vs after heap - object count
d$byStackOnly_percent <- round( (d$died_by_stack_only / d$died_by_stack) * 100, digits = 1 )
d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$died_by_stack ) * 100, digits = 1 )
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.stack.melt <- melt(d[,c( "benchmark", "byStackOnly_percent", "byStackAfterHeap_percent" )])
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/06-percent-deathcause-stack-only.pdf"
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#B2DF8A", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack breakdown"))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack breakdown"))
    }, finally = {
    }
)

print("======================================================================")
d <- xcsv
# Sort the benchmarks
# Drill down to stack only - by stack only vs after heap - object count
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.stack.melt <- melt(d[,c( "benchmark", "died_by_stack_only", "died_by_stack_after_heap" )])
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/06-actual-deathcause-stack-only.pdf"
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#B2DF8A", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack breakdown - actual"))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack breakdown - actual"))
    }, finally = {
    }
)

print("======================================================================")
print("    DONE.")
flush.console()
