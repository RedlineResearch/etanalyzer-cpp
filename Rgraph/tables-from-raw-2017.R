#=====================================================================
# Table of results for ISMM 2017
#    - Raoul Veroy
#=====================================================================
library(xtable)
library(stringr)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
max.livesize <- as.numeric(cargs[2])/(1024*1024)
tmp1 <- unlist(strsplit(datafile, split = "-", fixed = TRUE))
tmp2 <- unlist(strsplit(tmp1[[2]], split = ".", fixed = TRUE))
benchmark <- tmp2[[1]]
latex.out <- paste0(benchmark, "-table.tex")

xcsv <- read.table( datafile, sep = ",", header = TRUE )
str(xcsv)
# Add derived information
# NONE so far?

# Structure of table:
#     $ size     : int  
#     $ objects  : int  
#     $ groups   : int  
#     $ min_group: int  
#     $ max_group: int  
#     $ min_age  : int  
#     $ max_age  : int  
#     $ by_heap  : int  
#     $ allocsite: Factor
#     $ deathsite: Factor
#     $ allocpackage: Factor
#     $ deathpackage: Factor
print("======================================================================")
xcsv$percentage <- ((xcsv$size / max.livesize) * 100.0)
t1 <- xcsv[c( "allocsite",
              "deathsite",
              "size",
              "percentage",
              "objects",
              "min_age",
              "max_age" )]

print("======================================================================")

t1.2 <- t1
t1.2$size <- formatC( t1$size, format = "f", digits = 2, big.mark = "," )
t1.2$percentage <- formatC( t1$percentage, format = "f", digits = 2, big.mark = "," )
t1.2$objects <- formatC( t1$objects, format = "d", big.mark = "," )
t1.2$min_age <- formatC( t1$min_age, format = "f", digits = 2, big.mark = "," )
t1.2$max_age <- formatC( t1$max_age, format = "f", digits = 2, big.mark = "," )
print("======================================================================")

t1.2
# Caption for the table
# cap1 <- c('Insert caption HERE.',
cap1 <- c(paste0("Top 10 allocation-death context pairs for ", benchmark))

# Latex label
lab1 <- paste0("table-allocdeath-", benchmark)


# Create the table
x1 <- xtable( t1.2,
              caption = cap1, 
              label = lab1 )

names(x1) <- c( "Allocation site", "Death site",
                "\\makecell{Size \\\\ (MB)}", "\\% of total", "Objects",
                "\\makecell{Min \\\\ age(kB)}", "\\makecell{Max \\\\ age(kB)}" )
align(x1) <- c("l|", "l|", "l|", "r", "r", "r", "r", "r|")
print("======================================================================")
print.xtable( x1, type = "latex", file = latex.out,
              include.rownames = FALSE,
              floating.environment = "table*",
              table.placement = "t",
              sanitize.colnames.function = identity )
print("======================================================================")
print("    DONE.")
flush.console()
