#=====================================================================
# Plot the ISMM 2017 data
#    - Raoul Veroy
#=====================================================================
library(ggplot2)
library(lattice)
library(reshape2)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
targetdir <- cargs[2]
# datafile <- "/data/rveroy/pulsrc/data-ismm-2016/all.csv"
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add derived information
xcsv$byHeap_percent <- round( (xcsv$died_by_heap / xcsv$total_objects) * 100, digits = 2 )
xcsv$byStack_percent <- round( (xcsv$died_by_stack / xcsv$total_objects) * 100, digits = 2 )
xcsv$byHeap_and_Stack_percent <- xcsv$byHeap_percent + xcsv$byStack_percent
xcsv$atEnd_percent <- round( (xcsv$died_at_end / xcsv$total_objects) * 100, digits = 2 )
xcsv$byHeap_percent_noend <- round( (xcsv$died_by_heap / (xcsv$total_objects - xcsv$died_at_end)) * 100, digits = 2 )
xcsv$byStack_percent_noend <- round( (xcsv$died_by_stack / (xcsv$total_objects - xcsv$died_at_end)) * 100, digits = 2 )
xcsv$totalSize <- xcsv$died_by_stack_size + xcsv$died_by_heap_size
xcsv$totalSize_MB <- xcsv$totalSize / (1024*1024)
xcsv$max_live_size_MB <- xcsv$max_live_size / (1024*1024)

print("===[ 1: XCSV ]========================================================")
xcsv

print("======================================================================")
#--------------------------------------------------
# MAX LIVE SIZE GRAPH
print("Max live size barplot")
flush.console()
# maxlivesize.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-00-maxlivesize-barplot.pdf"
maxlivesize.out <- paste0( targetdir, "ALL-00-maxlivesize-barplot.pdf" )
maxlivesize.out.png <- paste0( targetdir, "ALL-00-maxlivesize-barplot.png" )
xlabel <- "Benchmark"
ylabel <- "Size MB"
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size_MB), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = max_live_size_MB)) +
             geom_bar( stat = "identity" ) # + coord_flip()
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        ggsave( filename = maxlivesize.out, plot = p )
        ggsave( filename = maxlivesize.out.png, plot = p )
    }, warning = function(w) {
        print(paste("WARNING: failed on max live size barplot"))
    }, error = function(e) {
        print(paste("ERROR: failed on max live size barplot"))
    }, finally = {
    }
)

print("======================================================================")
#--------------------------------------------------
print("====[ 2: Object count barplot - linear ]==============================")
# Object count linear scale
print("")
flush.console()
# objcount.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-01-object_barplot.pdf"
objcount.out <- paste0( targetdir, "ALL-01-object_barplot.pdf" )
objcount.out.png <- paste0( targetdir, "ALL-01-object_barplot.png" )
xlabel <- "Benchmark"
ylabel <- "Objects"
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = total_objects)) +
             geom_bar( stat = "identity" ) # + coord_flip()
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        ggsave( filename = objcount.out, plot = p )
        ggsave( filename = objcount.out.png, plot = p )
    }, warning = function(w) {
        print(paste("WARNING: failed on object count - linear scale"))
    }, error = function(e) {
        print(paste("ERROR: failed on object count - linear scale"))
    }, finally = {
    }
)

#--------------------------------------------------
# Size
print("====[ 2: Total size MB barplot ]======================================")
print("Total size barplot")
totalsize.out.mb <- paste0( targetdir, "ALL-02-totalsize-mb-barplot.pdf" )
totalsize.out.mb.png <- paste0( targetdir, "ALL-02-totalsize-mb-barplot.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Total size allocated in MB"
d <- xcsv
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
result = tryCatch( {
        p <- ggplot( d, aes(x = benchmark, y = totalSize_MB )) +
             geom_bar( stat = "identity" ) # + coord_flip()
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        ggsave(filename = totalsize.out.mb, plot = p)
        ggsave(filename = totalsize.out.mb.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on size barplot"))
    }, error = function(e) {
        print(paste("ERROR: failed on size barplot"))
    }, finally = {
    }
)

#--------------------------------------------------
# Died by heap vs stack vs at END percentage  stacked plot - object count
print("======================================================================")
print("Death by stack vs by heap")
deathreason.out <- paste0( targetdir, "ALL-04-percent-deathcause-object-count.pdf" )
deathreason.out.png <- paste0( targetdir, "ALL-04-percent-deathcause-object-count.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage of objects"
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeap_percent", "byStack_percent", "atEnd_percent")])
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + geom_bar( stat = "identity" ) # + coord_flip()
        # Use colors:
        #     ByHeap  = "#E41A1C"
        #     ByStack = "#A6CEE3"
        #     AtEnd   = "#7FC97F"
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3", "#4DAF4A"),
                                    name = "% of objects died",
                                    labels = c("By heap", "By stack", "Program end") )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        ggsave(filename = deathreason.out, plot = p)
        ggsave(filename = deathreason.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

#--------------------------------------------------
# Died by heap vs stack vs percentage (NO END stacked plot) - object count
print("======================================================================")
print("Death by stack vs by heap (NO END)")
deathreason.noend.out <- paste0( targetdir, "NOEND-04-percent-deathcause-object-count.pdf" )
deathreason.noend.out.png <- paste0( targetdir, "NOEND-04-percent-deathcause-object-count.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage of objects"
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeap_percent_noend", "byStack_percent_noend")])
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + geom_bar( stat = "identity" ) # + coord_flip()
        # Use colors:
        #     ByHeap  = "#E41A1C"
        #     ByStack = "#A6CEE3"
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3"),
                                    name = "% of objects died",
                                    labels = c("By heap", "By stack") )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        ggsave(filename = deathreason.noend.out, plot = p)
        ggsave(filename = deathreason.noend.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)

#--------------------------------------------------
#
print("======================================================================")

deathreason.out <- paste0( targetdir, "ALL-03-actual-deathcause-object-count.pdf" )
deathreason.out.png <- paste0( targetdir, "ALL-03-actual-deathcause-object-count.png" )
xlabel <- "Benchmark"
ylabel <- "Objects"
d <- xcsv
# Sort the benchmarks
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "died_by_heap", "died_by_stack", "died_at_end")])
# DEBUG:
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p +  geom_bar( stat = "identity" ) + coord_flip()
        # Use colors:
        #     ByHeap  = "#E41A1C"
        #     ByStack = "#A6CEE3"
        #     AtEnd   = "#7FC97F"
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3", "#4DAF4A"),
                                    name = "Objects died",
                                    labels = c("By heap", "By stack", "Program end") )
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        ggsave(filename = deathreason.out, plot = p)
        ggsave(filename = deathreason.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap."))
    }, finally = {
    }
)
print("======================================================================")

# TODO HERE
stopifnot(FALSE)

#--------------------------------------------------
# Died by heap vs stack size percentage  stacked plot
print("Death by stack vs by heap - size")
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-06-percent-deathcause-size.pdf"
deathreason.out <- paste0( targetdir, "ALL-06-percent-deathcause-size.pdf" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage by size"
d <- xcsv
# Sort the benchmarks
d$byHeapSize_percent <- round( (d$died_by_heap_size / (d$died_by_heap_size + d$died_by_stack_size + d$died_at_end_size)) * 100, digits = 2 )
d$byStackSize_percent <- round( (d$died_by_stack_size / (d$died_by_heap_size + d$died_by_stack_size + d$died_at_end_size)) * 100, digits = 2 )
d$atEndSize_percent <- round( (d$died_at_end_size / (d$died_by_heap_size + d$died_by_stack_size + d$died_at_end_size)) * 100, digits = 2 )
d$benchmark <- factor(d$benchmark, levels = d[ order(d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeapSize_percent", "byStackSize_percent", "atEndSize_percent")])
d.actual.size.melt <- melt(d[,c("benchmark", "died_by_heap_size", "died_by_stack_size", "died_at_end_size")])
# DEBUG:
# Plot percentage breakdown heap vs stack - size
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3", "#4DAF4A"),
                                    name = "Size % died",
                                    labels = c("By heap", "By stack", "Program end") )
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

# Plot actual breakdown heap vs stack - size
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/ALL-05-actual-deathcause-size.pdf"
deathreason.out <- paste0( targetdir, "ALL-05-actual-deathcause-size.pdf" )
xlabel <- "Benchmark"
ylabel <- "Size in MB"
result = tryCatch( {
        p <- ggplot( d.actual.size.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3", "#4DAF4A"),
                                     name = "Died",
                                     labels = c("By heap", "By stack", "Program end") )
        p <- p + labs( x = xlabel, y = ylabel )
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
xcsv.stack.melt <- melt(d[,c( "benchmark", "byHeap_percent", "byStackAfterHeap_percent", "byStackOnly_percent", "atEnd_percent" )])

xlabel <- "Benchmark"
ylabel <- "Objects"
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/STACK-01-deathcause-stack.pdf"
deathreason.out <- paste0( targetdir, "STACK-01-deathcause-stack.pdf" )
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#E41A1C", "#C994C7", "#A6CEE3", "#4DAF4A"),
                                    name = "Objects died",
                                    labels = c("By heap",
                                               "By stack after heap",
                                               "By stack only",
                                               "Program end") )
        p <- p + labs( x = xlabel, y = ylabel )
        ggsave(filename = deathreason.out, plot = p)
        # ggsave(filename = deathreason.landscape.out, plot = p, width = 6, height = 4)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap - OBJECT COUNT"))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap - OBJECT COUNT"))
    }, finally = {
    }
)
print("======================================================================")

# No coord flip, same graph
deathreason.landscape.out <- paste0( targetdir, "STACK-01-deathcause-stack-landscape.pdf" )
result = tryCatch( {
    p <- ggplot( xcsv.stack.melt,
                 aes( x = benchmark,
                      y = value,
                      fill = variable ) )
    p <- p + geom_bar( stat = "identity" )
    p <- p + scale_fill_manual( values = c("#E41A1C", "#C994C7", "#A6CEE3", "#4DAF4A"),
                                name = "Objects died",
                                labels = c("By heap",
                                           "By stack after heap",
                                           "By stack only",
                                           "Program end") )
    p <- p + labs( x = xlabel, y = ylabel )
    p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
    ggsave(filename = deathreason.landscape.out, plot = p, width = 6, height = 4)
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
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/05-size-deathcause-stack.pdf"
deathreason.out <- paste0( targetdir, "05-size-deathcause-stack.pdf" )
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#4292C6", "#B2DF8A", "#33A02C") )
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
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/07-deathcause-heap.pdf"
deathreason.out <- paste0( targetdir, "07-deathcause-heap.pdf" )
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.heap.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#A6CEE3", "#4292C6", "#33A02C") )
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
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/05-actual-deathcause-stack.pdf"
deathreason.out <- paste0( targetdir, "05-actual-deathcause-stack.pdf" )
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#4292C6", "#B2DF8A", "#33A02C") )
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
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/07-actual-deathcause-heap.pdf"
deathreason.out <- paste0( targetdir, "07-actual-deathcause-heap.pdf" )
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.heap.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) ) + geom_bar( stat = "identity" ) + coord_flip() + scale_fill_manual( values = c("#A6CEE3", "#4292C6", "#33A02C") )
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
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/08-deathcause-heap-only.pdf"
deathreason.out <- paste0( targetdir, "08-deathcause-heap-only.pdf" )
xlabel <- "Benchmark"
ylabel <- "Percentage of objects"
flush.console()
result = tryCatch( {
        p <- ggplot( xcsv.heap.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#FDD49E", "#D7301F"),
                                    labels = c("After null",
                                               "To valid target") )
        p <- p + labs( x = xlabel, y = ylabel )
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
xcsv.stack.melt <- melt(d[,c( "benchmark", "byStackAfterHeap_percent", "byStackOnly_percent" )])
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/06-percent-deathcause-stack-only.pdf"
deathreason.out <- paste0( targetdir, "06-percent-deathcause-stack-only.pdf" )
xlabel <- "Benchmark"
ylabel <- "Percentage of objects"
result = tryCatch( {
        p <- ggplot( xcsv.stack.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + geom_bar( stat = "identity" ) + coord_flip()
        p <- p + scale_fill_manual( values = c("#FDD49E", "#08519C"),
                                    name = "% of objects died",
                                    labels = c("By stack after heap", "By stack only") )
        p <- p + labs( x = xlabel, y = ylabel )
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
# deathreason.out <- "/data/rveroy/pulsrc/data-ismm-2016/y-GRAPHS/06-actual-deathcause-stack-only.pdf"
deathreason.out <- paste0( targetdir, "06-actual-deathcause-stack-only.pdf" )
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
