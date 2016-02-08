#=====================================================================
# Table of results for ISMM 2016
#    - Raoul Veroy
#=====================================================================
# library(ggplot2)
# library(reshape2)
library(xtable)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
table.out <- "/data/rveroy/pulsrc/data-ismm-2016/x-TABLES/ALL-01-object_barplot.pdf"
title <- "Object Count"
xlabel <- "Number of objects allocated"
# width <- as.integer(cargs[4])
# height <- as.integer(cargs[5])

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add derived information
xcsv$died_by_heap_percent <- round( (xcsv$died_by_heap / xcsv$total_objects) * 100, digits = 2 )
xcsv$died_by_stack_percent <- round( (xcsv$died_by_stack / xcsv$total_objects) * 100, digits = 2 )
xcsv$total_size <- xcsv$died_by_stack_size + xcsv$died_by_heap_size
xcsv$benchmark <- factor( xcsv$benchmark, levels = xcsv[ order( xcsv$max_live_size), "benchmark" ] )
xcsv$died_by_heap_kb <- round( xcsv$died_by

print("======================================================================")
xcsv

print("======================================================================")

t1.2 <- t1
t1 <- xcsv[c("benchmark", "total_objects", "died_by_heap", "died_by_heap_perc", "died_by_stack", "died_by_stack_perc")]
t1.2$total_objects <- formatC( t1$total_objects, format = "d", big.mark = "," )
t1.2$died_by_heap <- formatC( t1$died_by_heap, format = "d", big.mark = "," )
t1.2$died_by_heap_perc <- formatC( t1$died_by_heap, format = "d", big.mark = "," )
t1.2$died_by_stack <- formatC( t1$died_by_stack, format = "d", big.mark = "," )
t1.2$died_by_stack_perc <- formatC( t1$died_by_stack, format = "d", big.mark = "," )
cap1 <- c("How objects died for the Dacapo and SpecJVM98 benchmarks. Benchmarks are ordered according to maximum live size. Total objects is the number of objects for the whole execution of the bencmark. Died by heap is the number of objects that died by heap action, while the percentage is over the total number of objects. Died by stack is the number of objects that died by stack action, and the percentage is also over the total number of objects. Sizes are in MB.",
          "How objects died for the Dacapo and SpecJVM98 benchmarks.")
# Latex label
lab1 <- "tableOverview"
# x1 <- xtable()
print("======================================================================")
print("    DONE.")
flush.console()
