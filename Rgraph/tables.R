#=====================================================================
# Table of results for ISMM 2016
#    - Raoul Veroy
#=====================================================================
library(xtable)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
latex.out <- cargs[2]
table.out <- "/data/rveroy/pulsrc/data-ismm-2016/x-TABLES/tables.tex"

xcsv <- read.table( datafile, sep = ",", header = TRUE )
# Add derived information
xcsv$died_by_heap_perc <- round( (xcsv$died_by_heap / xcsv$total_objects) * 100, digits = 2 )
xcsv$died_by_stack_perc <- round( (xcsv$died_by_stack / xcsv$total_objects) * 100, digits = 2 )
xcsv$died_by_stack_size <- round( xcsv$died_by_stack_size / (1024*1024) )
xcsv$died_by_heap_size <- round( xcsv$died_by_heap_size / (1024*1024) )
xcsv$total_size <- xcsv$died_by_stack_size + xcsv$died_by_heap_size
# xcsv$total_size_mb <- round( xcsv$total_size / (1024*1024) )
xcsv$benchmark <- factor( xcsv$benchmark, levels = xcsv[ order( xcsv$max_live_size), "benchmark" ] )
xcsv$died_by_heap_size_perc <- round( (xcsv$died_by_heap_size / xcsv$total_size) * 100, digits = 2 )
xcsv$died_by_stack_size_perc <- round( (xcsv$died_by_stack_size / xcsv$total_size) * 100, digits = 2 )

print("======================================================================")
xcsv
t1 <- xcsv[c( "benchmark",
              "total_objects",
              "died_by_heap", "died_by_heap_perc",
              "died_by_stack", "died_by_stack_perc",
              # "total_size_mb",
              "died_by_heap_size", "died_by_heap_size_perc",
              "died_by_stack_size", "died_by_stack_size_perc" ) ]
print("======================================================================")

t1.2 <- t1
t1.2$total_objects <- formatC( t1$total_objects, format = "d", big.mark = "," )
# Format the died by heap/stack columns for object counts
t1.2$died_by_heap <- formatC( t1$died_by_heap, format = "d", big.mark = "," )
t1.2$died_by_stack <- formatC( t1$died_by_stack, format = "d", big.mark = "," )
t1.2$died_by_heap_perc <- formatC( t1$died_by_heap_perc, digits = 2, format = "f" )
t1.2$died_by_stack_perc <- formatC( t1$died_by_stack_perc, digits = 2, format = "f" )
# Format the died by heap/stack columns for size
t1.2$died_by_heap_size <- formatC( t1$died_by_heap_size, format = "d", big.mark = "," )
t1.2$died_by_stack_size <- formatC( t1$died_by_stack_size, format = "d", big.mark = "," )
t1.2$died_by_heap_size_perc <- formatC( t1$died_by_heap_size_perc, digits = 2, format = "f" )
t1.2$died_by_stack_size_perc <- formatC( t1$died_by_stack_size_perc, digits = 2, format = "f" )

t1.2
# Caption for the table
cap1 <- c("How objects died for the Dacapo and SpecJVM98 benchmarks. Benchmarks are ordered according to maximum live size. Total objects is the number of objects for the whole execution of the bencmark. Died by heap is the number of objects that died by heap action, while the percentage is over the total number of objects. Died by stack is the number of objects that died by stack action, and the percentage is also over the total number of objects. Sizes are in MB. Percentages are rounded off to two decimal digits.",
          "How objects died in the Dacapo and SpecJVM98 benchmarks.")

# Latex label
lab1 <- "tableOverview"

# Create the table
x1 <- xtable( t1.2,
              caption = cap1, 
              label = lab1 )
print("======================================================================")
print.xtable( x1, type = "latex", file = latex.out,
              include.rownames = FALSE )
print("======================================================================")
print("    DONE.")
flush.console()
