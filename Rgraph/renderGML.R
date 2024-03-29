#=====================================================================
# renderGML
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
gmain <- read.graph( tgtfile, format = "gml" )
glist <- decompose.graph( gmain, mode = "weak" )
png(filename = outfile, width = width, height = height)
l <- layout.drl(glist[[1]], options = list(simmer.attraction = 0))
# layout = layout.fruchterman.reingold(glist[[1]], niter = 10000),
plot( glist[[1]],
      layout = l,
      vertex.color = V(gmain)$color,
      vertex.size = 5,
      vertex.label = NA,
      edge.arrow.size = 0.5 )
dev.off()
