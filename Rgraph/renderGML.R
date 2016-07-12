#=====================================================================
# renderGraphML
#   args:
#       source GraphML file
#       output png file
#       width
#       height
#=====================================================================
library(igraph)
library(ggplot2)

# NOTES:
# to access a vertex:
#      V(g)[elist[3,2]]$type

cargs <- commandArgs(TRUE)
tgtfile <- cargs[1]
outfile <- cargs[2]
width <- as.integer(cargs[3])
height <- as.integer(cargs[4])
sumdata <- cargs[5]
gmain <- read.graph( tgtfile, format = "graphml" )
png(filename = outfile, width = width, height = height)
plot( gmain, layout=layout.fruchterman.reingold, vertex.color=V(gmain)$color, vertex.size = 5, vertex.label = NA, edge.arrow.size = 0.5 )
dev.off()
