library(ggplot2)

cargs <- commandArgs(TRUE)
datafile <- cargs[1]
bmark <- cargs[2]
outdir <- cargs[3]

data <- read.csv( datafile )
setwd(outdir)

# Try grouping
d.seq <- seq( from = min(data$num_objects),
              to = max(data$num_objects),
              by = (max(data$num_objects) - min(data$num_objects))/ 30 )
d <- aggregate( data$size_bytes, list(cut(data$num_objects, breaks = d.seq)), sum )

colnames(d) <- c("num_objects", "size_bytes")

p <- ggplot( d, aes( x = num_objects, y = size_bytes ) ) + geom_bar( stat = "identity" )  + theme( axis.text.x = element_blank() )
ggsave( filename = paste0(bmark, "-grouped-barplot.png"), plot = p, width = 4, height = 4 )

# Density plots
p <- ggplot( data, aes( x = size_bytes ) ) + geom_density()
ggsave( filename = paste0(bmark, "-density.png"), plot = p, width = 4, height = 4 )
#     - Try log scale
p <- p + scale_y_log10()
ggsave( filename = paste0(bmark, "-density-log.png"), plot = p, width = 4, height = 4 )


# result = tryCatch( {
#         p <- ggplot( data, aes( x = num_objects, y = size_bytes)) + geom_bar( stat = "identity" ) + scale_y_log10() + coord_flip()
#         ggsave( filename = paste0(bmark, "-barplot.png"), plot = p, width = 4, height = 4 )
#     }, warning = function(w) {
#         print(paste("WARNING: failed on barplot"))
#     }, error = function(e) {
#         print(paste("ERROR: failed on barplot"))
#     }, finally = {
#     }
# )
