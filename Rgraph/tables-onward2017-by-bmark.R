#=====================================================================
# Table of results for ISMM 2017
# * Slice by benchmark instead of type signature
#    - Raoul Veroy
#=====================================================================
library(xtable)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
latex.out <- cargs[2]
primary <- cargs[3]
table.out <- paste0("/data/rveroy/pulsrc/onwards-2017/Tables/BUILD", latex.out)

xcsv <- read.table( datafile, sep = ",", header = TRUE )
print("======================================================================")
xcsv
t1 <- xcsv
print("======================================================================")


t1.2 <- t1
t1.2$number_of_cycles <- formatC( t1$number_of_cycles, format = "d", big.mark = "," )
t1.2$minimum <- formatC( t1$minimum, format = "d", big.mark = "," )
t1.2$maximum <- formatC( t1$maximum, format = "d", big.mark = "," )
t1.2$median <- formatC( t1$median, format = "d", big.mark = "," )

t1.2
# Caption for the table
cap1 <- c('TODO \\emph{How to emph}.',
          "Probably the title.")

# Latex label
lab1 <- "tableByType"

# Create the table
x1 <- xtable( t1.2,
              caption = cap1, 
              label = lab1 )

names(x1) <- c( primary, "Number of cycles",
                "Minimum", "Maximum",
                "Median" )
                
print("======================================================================")
print.xtable( x1, type = "latex", file = latex.out,
              include.rownames = FALSE,
              floating.environment = "table*" )
print("======================================================================")
print("    DONE.")
flush.console()
