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
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add derived information
xcsv$byHeap_percent <- round( (xcsv$died_by_heap / xcsv$total_objects) * 100, digits = 2 )
xcsv$byStack_percent <- round( (xcsv$died_by_stack / xcsv$total_objects) * 100, digits = 2 )
xcsv$totalSize <- xcsv$died_by_stack_size + xcsv$died_by_heap_size
xcsv$totalSize_MB <- xcsv$totalSize / (1024*1024)

print("======================================================================")
xcsv

print("======================================================================")
#--------------------------------------------------
# Object count linear scale
print("Object count barplot - linear")
flush.console()
objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-01-object_barplot.pdf"
xlabel <- "Benchmark"
ylabel <- "Objects"
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = total_objects)) +
             geom_bar( stat = "identity" ) + coord_flip()
        p <- p + labs( x = xlabel, y = ylabel )
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
objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-02-size-barplot.pdf"
flush.console()
xlabel <- "Benchmark"
ylabel <- "Size allocated in MB"
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = totalSize_MB )) +
             geom_bar( stat = "identity" ) + coord_flip()
        p <- p + labs( x = xlabel, y = ylabel )
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
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-04-percent-deathcause-object-count.pdf"
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage of objects"
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeap_percent", "byStack_percent")])
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#1F78B4", "#33A02C"),
                                    name = "% of objects died",
                                    labels = c("By heap", "By stack"))
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)
print("======================================================================")

deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-03-actual-deathcause-object-count.pdf"
xlabel <- "Benchmark"
ylabel <- "Objects"
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "died_by_heap", "died_by_stack")])
# DEBUG:
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p +  geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#1F78B4", "#33A02C"),
                                    name = "Objects died",
                                    labels = c("By heap", "By stack") )
        p <- p + labs( x = xlabel, y = ylabel )
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
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-06-percent-deathcause-size.pdf"
flush.console()
d <- xcsv
# Sort the benchmarks
d$byHeapSize_percent <- round( (d$died_by_heap_size / (d$died_by_heap_size + d$died_by_stack_size)) * 100, digits = 2 )
d$byStackSize_percent <- round( (d$died_by_stack_size / (d$died_by_heap_size + d$died_by_stack_size)) * 100, digits = 2 )
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
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-05-actual-deathcause-size.pdf"
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
# ---------------------------------------------------------------------------
#     END ALL TYPE GRAPHS
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
#     BEGIN STACK FOCUS GRAPHS
# ---------------------------------------------------------------------------

print("======================================================================")
#--------------------------------------------------
# Died by heap vs stack percentage  stacked plot - object count
# with stack split into by 
# - stack only
# - stack after heap
flush.console()
d <- xcsv
# Sort the benchmarks
d$byStackOnly_percent <- round( (d$died_by_stack_only / d$total_objects) * 100, digits = 2 )
d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$total_objects) * 100, digits = 2 )
d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null_heap) / d$total_objects) * 100, digits = 2 )
d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$total_objects) * 100, digits = 2 )
d$benchmark <- factor( d$benchmark, levels = d[ order( d$max_live_size), "benchmark" ] )
xcsv.heap.melt <- melt(d[,c( "benchmark", "byHeapAfterNull_percent", "byHeapAfterValid_percent", "byStack_percent" )])
xcsv.stack.melt <- melt(d[,c( "benchmark", "byHeap_percent", "byStackOnly_percent", "byStackAfterHeap_percent" )])

print("DEBUG A:")
xcsv.stack.melt
print("END DEBUG A.")
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/STACK-01-deathcause-stack.pdf"
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#1F78B4", "#B2DF8A", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap - OBJECT COUNT"))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap - OBJECT COUNT"))
    }, finally = {
    }
)
print("======================================================================")

#--------------------------------------------------
# HERE: Died by heap vs stack percentage  stacked plot - size
# with stack split into by 
# - stack only
# - stack after heap
flush.console()
print("DEBUG:")
d <- xcsv
# Sort the benchmarks
# d$byStackOnly_percent <- round( (d$died_by_stack_only / d$total_objects) * 100, digits = 2 )
# d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$total_objects) * 100, digits = 2 )
# d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$total_objects) * 100, digits = 2 )
# d$benchmark <- factor( d$benchmark, levels = d[ order( d$max_live_size), "benchmark" ] )
d$last_update_valid_heap_size <- d$died_by_heap_size - d$last_update_null_heap_size
d
xcsv.heap.melt <- melt(d[,c( "benchmark", "last_update_null_heap_size", "last_update_valid_heap_size", "died_by_stack_size" )])
xcsv.stack.melt <- melt(d[,c( "benchmark", "died_by_heap_size", "died_by_stack_only_size", "died_by_stack_after_heap_size" )])

print("======================================================================")
print("Death by stack broken down")
flush.console()
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/05-size-deathcause-stack.pdf"
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#1F78B4", "#B2DF8A", "#33A02C") )
        ggsave(filename = deathreason.out, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap - SIZE."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap - SIZE."))
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
print("======================================================================")
#--------------------------------------------------
# Died by heap vs stack stacked plot - actual object count
# with stack split into by 
# - stack only
# - stack after heap
flush.console()
d <- xcsv
# Sort the benchmarks
# d$byStackOnly_percent <- round( (d$died_by_stack_only / d$total_objects) * 100, digits = 2 )
# d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$total_objects) * 100, digits = 2 )
# d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null_heap) / d$total_objects) * 100, digits = 2 )
# d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$total_objects) * 100, digits = 2 )
d$last_update_valid_heap <- d$died_by_heap - d$last_update_null_heap
d$benchmark <- factor( d$benchmark, levels = d[ order( d$max_live_size), "benchmark" ] )
xcsv.heap.melt <- melt(d[,c( "benchmark", "last_update_null_heap", "last_update_valid_heap", "died_by_stack" )])
xcsv.stack.melt <- melt(d[,c( "benchmark", "died_by_heap", "died_by_stack_only", "died_by_stack_after_heap" )])

print("======================================================================")
print("Death by stack broken down")
flush.console()
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/05-actual-deathcause-stack.pdf"
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
deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/07-actual-deathcause-heap.pdf"
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
d$byHeapAfterNull_percent <- round( (d$last_update_null_heap / d$died_by_heap) * 100, digits = 2 )
d$byHeapAfterValid_percent <- round( ((d$died_by_heap - d$last_update_null_heap) / d$died_by_heap) * 100, digits = 2 )
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
d$byStackOnly_percent <- round( (d$died_by_stack_only / d$died_by_stack) * 100, digits = 2 )
d$byStackAfterHeap_percent <- round( (d$died_by_stack_after_heap / d$died_by_stack ) * 100, digits = 2 )
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
