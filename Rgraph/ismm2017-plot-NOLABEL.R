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
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add derived information. We only consider size here
xcsv$totalSize_noend <- xcsv$died_by_stack_size + xcsv$died_by_heap_size
xcsv$totalSize <- xcsv$totalSize_noend + xcsv$died_at_end_size
xcsv$byHeap_percent <- round( (xcsv$died_by_heap_size / xcsv$totalSize) * 100, digits = 2 )
xcsv$byStack_percent <- round( (xcsv$died_by_stack_size / xcsv$totalSize) * 100, digits = 2 )
xcsv$atEnd_percent <- round( (xcsv$died_at_end_size / xcsv$totalSize) * 100, digits = 2 )
xcsv$byHeap_percent_noend <- round( (xcsv$died_by_heap_size / xcsv$totalSize_noend) * 100, digits = 2 )
xcsv$byStack_percent_noend <- round( (xcsv$died_by_stack_size / xcsv$totalSize_noend) * 100, digits = 2 )
xcsv$byStack_percent_noend <- round( (xcsv$died_by_stack_size / xcsv$totalSize_noend) * 100, digits = 2 )
# Incorporatin stack after heap
xcsv$died_by_heap_with_sheap_size <- xcsv$died_by_heap_size + xcsv$died_by_stack_after_heap_size
xcsv$died_by_stack_only_size <- xcsv$died_by_stack_size - xcsv$died_by_stack_after_heap_size
xcsv$bySAHeap_percent_noend <- round( (xcsv$died_by_stack_after_heap_size / xcsv$totalSize_noend) * 100, digits = 2 )
xcsv$byHeap_SHeap_percent_noend <- round( (xcsv$died_by_heap_with_sheap_size / xcsv$totalSize_noend) * 100, digits = 2 )
xcsv$byStack_only_percent_noend <- round( (xcsv$died_by_stack_only_size / xcsv$totalSize_noend) * 100, digits = 2 )
# xcsv$totalSize_MB <- xcsv$totalSize / (1024*1024)
# xcsv$max_live_size_MB <- xcsv$max_live_size / (1024*1024)

print("===[ 1: XCSV ]========================================================")
xcsv

print("======================================================================")
#--------------------------------------------------
# Died by heap vs stack vs at END percentage  stacked plot - size
print("======================================================================")
print("Death by stack vs by heap vs at end")
deathreason.out <- file.path( targetdir, "01-percent-deathcause.pdf" )
deathreason.out.png <- file.path( targetdir, "01-percent-deathcause.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage by size"
d <- xcsv
# Sort the benchmarks - descending order
d$benchmark <- factor(d$benchmark, levels = d[ order(-d$max_live_size), "benchmark"])
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
        # or try 4DAF4A
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3", "#7FC97F"),
                                    name = "% in bytes",
                                    labels = c("By heap", "By stack", "Program end") )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        p <- p + guides( fill = FALSE )
        ggsave(filename = deathreason.out, plot = p)
        ggsave(filename = deathreason.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap vs progend."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap vs progend."))
    }, finally = {
    }
)

#--------------------------------------------------
# Died by heap vs stack vs (NO PROG END)  stacked plot - size
print("======================================================================")
print("Death by stack vs by heap (no end)")
deathreason.out <- file.path( targetdir, "02-percent-deathcause-NOEND.pdf" )
deathreason.out.png <- file.path( targetdir, "02-percent-deathcause-NOEND.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage by size"
d <- xcsv
# Sort the benchmarks - descending order
d$benchmark <- factor(d$benchmark, levels = d[ order(-d$max_live_size), "benchmark"])
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
        # or try 4DAF4A
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3"),
                                    name = "% in bytes",
                                    labels = c("By heap", "By stack") )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        p <- p + guides( fill = FALSE )
        ggsave(filename = deathreason.out, plot = p)
        ggsave(filename = deathreason.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack vs heap vs progend."))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack vs heap vs progend."))
    }, finally = {
    }
)

#--------------------------------------------------
# Died by heap with sheap vs stack only (NO PROG END)  stacked plot - size
print("======================================================================")
print("Death by stack only vs by heap with sheap ")
deathreason.out <- file.path( targetdir, "03-percent-deathcause-sheap-NOEND.pdf" )
deathreason.out.png <- file.path( targetdir, "03-percent-deathcause-sheap-NOEND.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage by size"
d <- xcsv
# Sort the benchmarks - descending order
d$benchmark <- factor(d$benchmark, levels = d[ order(-d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeap_SHeap_percent_noend", "byStack_only_percent_noend")])
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
        # or try 4DAF4A
        p <- p + scale_fill_manual( values = c("#E41A1C", "#A6CEE3"),
                                    name = "% in bytes",
                                    labels = c("By heap with stack after heap", "By stack only") )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        p <- p + guides( fill = FALSE )
        ggsave(filename = deathreason.out, plot = p)
        ggsave(filename = deathreason.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack only vs heap + sheap"))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack only vs heap + sheap"))
    }, finally = {
    }
)

#--------------------------------------------------
# Died by heap vs sheap vs stack only (NO PROG END)  stacked plot - size
print("======================================================================")
print("Death by stack only vs by heap vs sheap ")
deathreason.out <- file.path( targetdir, "04-percent-deathcause-sheap-NOEND.pdf" )
deathreason.out.png <- file.path( targetdir, "04-percent-deathcause-sheap-NOEND.png" )
flush.console()
xlabel <- "Benchmark"
ylabel <- "Percentage by size"
d <- xcsv
# Sort the benchmarks - descending order
d$benchmark <- factor(d$benchmark, levels = d[ order(-d$max_live_size), "benchmark"])
xcsv.melt <- melt(d[,c("benchmark", "byHeap_percent_noend", "bySAHeap_percent_noend", "byStack_only_percent_noend")])
result = tryCatch( {
        p <- ggplot( xcsv.melt,
                     aes( x = benchmark,
                          y = value,
                          fill = variable ) )
        p <- p + labs( x = xlabel, y = ylabel )
        p <- p + geom_bar( stat = "identity" ) # + coord_flip()
        # Use colors:
        #     ByHeap  = "#E41A1C"
        #     BySHeap = "#4DAF4A"
        #     ByStack = "#A6CEE3"
        p <- p + scale_fill_manual( values = c("#E41A1C", "#4DAF4A", "#A6CEE3"),
                                    name = "% in bytes",
                                    labels = c("By heap", "By stack after heap", "By stack only") )
        p <- p + theme(axis.text.x = element_text(angle = 90, hjust = 1))
        p <- p + guides( fill = FALSE )
        ggsave(filename = deathreason.out, plot = p)
        ggsave(filename = deathreason.out.png, plot = p)
    }, warning = function(w) {
        print(paste("WARNING: failed on death reason stack only vs heap vs sheap"))
    }, error = function(e) {
        print(paste("ERROR: failed on death reason stack only vs heap vs sheap"))
    }, finally = {
    }
)


print("======================================================================")
print("    DONE.")
flush.console()
