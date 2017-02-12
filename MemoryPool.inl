/*-
 * Copyright (c) 2013 Cosku Acay, http://www.coskuacay.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#ifndef MEMORY_BLOCK_TCC
#define MEMORY_BLOCK_TCC

#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <time.h>
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using std::string;
using boost::lexical_cast;
using boost::uuids::random_generator;
using boost::uuids::uuid;

//------------------------------------------------------------------------------
//   Utility functions
//------------------------------------------------------------------------------
string make_uuid()
{
    time_t seconds = time(0);
    string ts = lexical_cast< string >( seconds );
    ts += "-";
    return ts + lexical_cast< string >((random_generator())());
}


int open_sparse_tempfile( string &basedir,
                          size_t mysize,
                          char *tmpbuf )
{
    // Create temp filename
    char sep = '/';
    string tempname;
    if (basedir[basedir.length() - 1] == sep) {
        tempname = basedir + make_uuid();
    } else {
        tempname = basedir + sep + make_uuid();
    }
    // open for the file descriptor
    int fdesc = open( tempname.c_str(), O_CREAT | O_RDWR );
    // fopen the file for read/write, truncating if it exists.
    FILE *fptr = fdopen( fdesc, "w+" );
    // fseek to the desired file size - 1.
    // int result = fseek( fptr, mysize - 1, SEEK_CUR );
    int result = fwrite( tmpbuf, 1, mysize, fptr );
    // TODO: Check for -1 => Out of Memory error.
    assert(result == mysize);
    fclose(fptr);
    fdesc = open( tempname.c_str(), O_RDWR );
    return fdesc;
}

//------------------------------------------------------------------------------

// DOC: TODO
template< typename T, size_t BlockSize>
inline typename MemoryPool<T, BlockSize>::size_type
MemoryPool<T, BlockSize>::padPointer( data_pointer_t p,
                                      size_type align ) const noexcept
{
    uintptr_t result = reinterpret_cast< uintptr_t >(p);
    return ((align - result) % align);
}

// DOC: TODO
template< typename T, size_t BlockSize>
MemoryPool<T, BlockSize>::MemoryPool( string &basedir ) noexcept
    : currentBlock_(nullptr)
    , currentSlot_(nullptr)
    , lastSlot_(nullptr)
    , freeSlots_(nullptr)
    , basedir_(basedir)
    , tmpbuf_(nullptr)
{
}

// DOC: TODO
template< typename T, size_t BlockSize>
MemoryPool<T, BlockSize>::MemoryPool( const MemoryPool& ) noexcept
    : MemoryPool()
{
}

// DOC: TODO
template< typename T, size_t BlockSize>
MemoryPool<T, BlockSize>::MemoryPool( MemoryPool &&mempool ) noexcept
    : currentBlock_(mempool.currentBlock_)
    , currentSlot_(mempool.currentSlot_)
    , lastSlot_(mempool.lastSlot_)
    , freeSlots_(mempool.freeSlots)
    , basedir_(mempool.basedir_)
{
    mempool.currentBlock_ = nullptr;
}

// DOC: TODO
template< typename T, size_t BlockSize >
template< class U >
MemoryPool<T, BlockSize>::MemoryPool( const MemoryPool<U>& ) noexcept
    : MemoryPool()
{
}

// DOC: TODO
template< typename T, size_t BlockSize >
MemoryPool< T, BlockSize > &
MemoryPool<T, BlockSize>::operator=( MemoryPool&& mempool ) noexcept
{
    if (this != &mempool) {
        std::swap( currentBlock_,
                   mempool.currentBlock_ );
        this->currentSlot_ = mempool.currentSlot_;
        this->lastSlot_ = mempool.lastSlot_;
        this->freeSlots_ = mempool.freeSlots;
    }
    return *this;
}

// DOC: TODO
template< typename T, size_t BlockSize >
MemoryPool< T, BlockSize >::~MemoryPool() noexcept
{
    slot_pointer_t curr = currentBlock_;
    while (curr != nullptr) {
        slot_pointer_t prev = curr->next;
        // operator delete(reinterpret_cast< void * >(curr));
        munmap(reinterpret_cast< void * >(curr), BlockSize);
        curr = prev;
    }
}

// DOC: TODO
template< typename T, size_t BlockSize >
inline typename MemoryPool< T, BlockSize >::pointer_t
MemoryPool<T, BlockSize>::address(reference x) const noexcept
{
    return &x;
}

// DOC: TODO
template< typename T, size_t BlockSize >
inline typename MemoryPool< T, BlockSize >::const_pointer
MemoryPool< T, BlockSize >::address( const_reference x ) const noexcept
{
    return &x;
}

// DOC: TODO
template< typename T, size_t BlockSize >
void
MemoryPool< T, BlockSize >::allocateBlock()
{
    // Allocate space for the new block and store a pointer to the previous one
    data_pointer_t newBlock = reinterpret_cast<data_pointer_t>(operator new(BlockSize));
    reinterpret_cast< slot_pointer_t >(newBlock)->next = currentBlock_;
    currentBlock_ = reinterpret_cast< slot_pointer_t >(newBlock);
    // Pad block body to satisfy the alignment requirements for elements
    data_pointer_t body = newBlock + sizeof(slot_pointer_t);
    size_type bodyPadding = padPointer(body, alignof(slot_type_t));
    currentSlot_ = reinterpret_cast< slot_pointer_t >(body + bodyPadding);
    lastSlot_ = reinterpret_cast< slot_pointer_t >(newBlock + BlockSize - sizeof(slot_type_t) + 1);
}

// DOC: TODO
template< typename T, size_t BlockSize >
void
MemoryPool< T, BlockSize >::allocateBlock_mmap()
{
    // Allocate space for the new block and store a pointer to the previous one
    int fdesc = open_sparse_tempfile( this->basedir_,
                                      BlockSize,
                                      this->tmpbuf_ );
    void *vptr = mmap( NULL,
                       BlockSize,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE,
                       fdesc,
                       0 );
    data_pointer_t newBlock = reinterpret_cast<data_pointer_t>(vptr);
    reinterpret_cast< slot_pointer_t >(newBlock)->next = currentBlock_;
    currentBlock_ = reinterpret_cast< slot_pointer_t >(newBlock);
    // Pad block body to satisfy the alignment requirements for elements
    data_pointer_t body = newBlock + sizeof(slot_pointer_t);
    size_type bodyPadding = padPointer(body, alignof(slot_type_t));
    currentSlot_ = reinterpret_cast< slot_pointer_t >(body + bodyPadding);
    lastSlot_ = reinterpret_cast< slot_pointer_t >(newBlock + BlockSize - sizeof(slot_type_t) + 1);
}

// DOC: TODO
template< typename T, size_t BlockSize >
inline typename MemoryPool< T, BlockSize >::pointer_t
MemoryPool< T, BlockSize >::allocate( size_type n,
                                      const_pointer hint )
{
    if (this->freeSlots_ != nullptr) {
        pointer_t result = reinterpret_cast< pointer_t >( this->freeSlots_ );
        this->freeSlots_ = this->freeSlots_->next;
        return result;
    }
    else {
        if (this->currentSlot_ >= this->lastSlot_) {
            // this->allocateBlock();
            this->allocateBlock_mmap();
        }
        return reinterpret_cast< pointer_t >(this->currentSlot_++);
    }
}

// DOC: TODO
template< typename T, size_t BlockSize >
inline void
MemoryPool< T, BlockSize >::deallocate( pointer_t p,
                                        size_type n )
{
    if (p != nullptr) {
        reinterpret_cast< slot_pointer_t >(p)->next = this->freeSlots_;
        this->freeSlots_ = reinterpret_cast< slot_pointer_t >(p);
    }
}

// DOC: TODO
template< typename T, size_t BlockSize >
inline typename MemoryPool< T, BlockSize >::size_type
MemoryPool< T, BlockSize >::max_size() const noexcept
{
    size_type maxBlocks = (-1 / BlockSize);
    return ( (BlockSize - sizeof(data_pointer_t)) /
             (sizeof(slot_type_t) * maxBlocks) );
}

// DOC: TODO
template< typename T, size_t BlockSize >
template< class UType, class... Args >
inline void
MemoryPool<T, BlockSize>::construct( UType *p, Args &&... args )
{
    new (p) UType (std::forward<Args>(args)...);
}



template< typename T, size_t BlockSize >
template< class UType >
inline void
MemoryPool< T, BlockSize>::destroy( UType *p )
{
    p->~UType();
}

// DOC: TODO
template< typename T, size_t BlockSize >
template< class... Args >
inline typename MemoryPool< T, BlockSize >::pointer_t
MemoryPool< T, BlockSize >::newElement( Args &&... args )
{
    pointer_t result = allocate();
    construct< value_type >( result, std::forward<Args>(args)... );
    return result;
}

// DOC: TODO
template< typename T, size_t BlockSize >
inline void
MemoryPool< T, BlockSize >::deleteElement( pointer_t p )
{
    if (p != nullptr) {
        p->~value_type();
        deallocate(p);
    }
}

#endif // MEMORY_BLOCK_TCC
