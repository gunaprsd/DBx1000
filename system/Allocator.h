#pragma once 
#ifndef __SYSTEM_ALLOCATOR_H__
#define __SYSTEM_ALLOCATOR_H__

#include <map>
#include "Global.h"


const int SizeNum = 4;
const uint32_t BlockSizes[] = {32, 64, 256, 1024};

typedef struct FreeBlock
{
    uint32_t size;
    struct FreeBlock* next;
} FreeBlock;

typedef uint32_t ArenaId;

class Arena
{
public:
	void 	initialize	(ArenaId arena_id, uint32_t size);
	void *	allocate		();
	void 	free			(void * ptr);
private:
	char * 			_buffer;
	ArenaId			_arena_id;
	uint32_t 		_size_in_buffer;
	uint32_t			_block_size;
	FreeBlock * 		_head;
	char 			_padding[128 - sizeof(ArenaId) - 2 * sizeof(uint32_t) - sizeof(void *)*2 - 8];
};

class Allocator
{
public:
    void 	initialize		(uint64_t part_cnt, uint64_t bytes_per_part);
    void 	register_thread	(ThreadId thd_id);
    void 	unregister		();

    void * 		allocate			(uint64_t size, PartId part_id);
    void 		free				(void * block, uint64_t size);
	ArenaId 		get_arena_id		();
private:
    void 		initialize_thread_arena();
	int 			get_size_id(uint32_t size);
	
	// each thread has several arenas for different block size
	Arena ** 					_arenas;
	uint32_t 					_bucket_cnt;
    std::pair<pthread_t, int>* 	pid_arena;//                     max_arena_id;
    pthread_mutex_t         		map_lock; // only used for pid_to_arena update
};

#endif
