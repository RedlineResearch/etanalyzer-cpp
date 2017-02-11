// TODO: 
//    * Sizes should imitate the size type of malloc
//    * The alloc/free functions should imitate malloc/free
// API:
//
// unsigned int mem_pool_init( string &name, 
//                             unsigned int cell_size,
//                             unsigned int num_cells )
//      name - name of region
//      cell_size - size of each cell
//      num_cells - number of cells for the allocation region
//
// void *mem_pool_alloc( unsigned int newsize )
//      - Use sbrk to allocate.
//      - Increase resize policy should be 
//
// free( void *ptr )
