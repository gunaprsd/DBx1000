#include "Allocator.h"

#include "Global.h"
#include "Helper.h"

// Assume the data is strided across the L2 slices, stride granularity 
// is the size of a page
void Allocator::initialize(uint64_t part_cnt, uint64_t bytes_per_part) {
	if (g_thread_cnt < g_init_parallelism)
		_bucket_cnt = g_init_parallelism * 4 + 1;
	else
		_bucket_cnt = g_thread_cnt * 4 + 1;
	pid_arena = new std::pair<pthread_t, int>[_bucket_cnt];
	for (uint32_t i = 0; i < _bucket_cnt; i ++)
		pid_arena[i] = std::make_pair(0, 0);

	if (THREAD_ALLOC) {
		assert( !g_part_alloc );
		initialize_thread_arena();
	}
}

void Arena::initialize(ArenaId id, uint32_t size)
{
	_buffer = NULL;
	_arena_id = id;
	_size_in_buffer = 0;
	_head = NULL;
	_block_size = size;
}

void * Arena::allocate()
{
	FreeBlock * block;
	if (_head == NULL) {
		// not in the list. allocate from the buffer
		uint32_t size = (_block_size + sizeof(FreeBlock) + (MEM_ALLIGN - 1)) & ~(MEM_ALLIGN-1);
		if (_size_in_buffer < size) {
			_buffer = (char *) malloc(_block_size * 40960);
			_size_in_buffer = _block_size * 40960; // * 8;
		}
		block = (FreeBlock *)_buffer;
		block->size = _block_size;
		_size_in_buffer -= size;
		_buffer = _buffer + size;
	} else {
		block = _head;
		_head = _head->next;
	}
	return (void *) ((char *)block + sizeof(FreeBlock));
}

void Arena::free(void * ptr)
{
	FreeBlock * block = (FreeBlock *)((uint64_t)ptr - sizeof(FreeBlock));
	block->next = _head;
	_head = block;
}

void Allocator::initialize_thread_arena() {
	uint32_t buf_cnt = g_thread_cnt;
	if (buf_cnt < g_init_parallelism)
		buf_cnt = g_init_parallelism;
	_arenas = new Arena * [buf_cnt];
	for (uint32_t i = 0; i < buf_cnt; i++) {
		_arenas[i] = new Arena[SizeNum];
		for (int n = 0; n < SizeNum; n++) {
			assert(sizeof(Arena) == 128);
			_arenas[i][n].initialize(i, BlockSizes[n]);
		}
	}
}

void Allocator::register_thread(ThreadId id) {
	if (THREAD_ALLOC) {
		pthread_mutex_lock( &map_lock );
		pthread_t pid = pthread_self();
		int entry = pid % _bucket_cnt;
		while (pid_arena[ entry ].first != 0) {
			printf("conflict at entry %d (pid=%ld)\n", entry, pid);
			entry = (entry + 1) % _bucket_cnt;
		}
		pid_arena[ entry ].first = pid;
		pid_arena[ entry ].second = id;
		pthread_mutex_unlock( &map_lock );
	}
}

void Allocator::unregister()
{
	if (THREAD_ALLOC) {
		pthread_mutex_lock( &map_lock );
		for (uint32_t i = 0; i < _bucket_cnt; i ++) {
			pid_arena[i].first = 0;
			pid_arena[i].second = 0;
		}
		pthread_mutex_unlock( &map_lock );
	}
}

ArenaId  Allocator::get_arena_id()
{
	int arena_id; 
#if NOGRAPHITE
	pthread_t pid = pthread_self();
	int entry = pid % _bucket_cnt;
	while (pid_arena[entry].first != pid) {
		if (pid_arena[entry].first == 0)
			break;
		entry = (entry + 1) % _bucket_cnt;
	}
	arena_id = pid_arena[entry].second;
#else 
	arena_id = CarbonGetTileId();
#endif
	return arena_id;
}

int Allocator::get_size_id(uint32_t size)
{
	for (int i = 0; i < SizeNum; i++) {
		if (size <= BlockSizes[i]) 
			return i;
	}
    printf("size = %d\n", size);
	assert( false );
}


void Allocator::free(void * ptr, uint64_t size)
{
	if (NO_FREE) {} 
	else if (THREAD_ALLOC) {
		int arena_id = get_arena_id();
		FreeBlock * block = (FreeBlock *)((uint64_t)ptr - sizeof(FreeBlock));
		int size = block->size;
		int size_id = get_size_id(size);
		_arenas[arena_id][size_id].free(ptr);
	} else {
		std::free(ptr);
	}
}

//TODO the program should not access more than a PAGE
// to guanrantee correctness
// lock is used for consistency (multiple threads may alloc simultaneously and 
// cause trouble)
void * Allocator::allocate(uint64_t size, uint64_t part_id)
{
	void * ptr;
    if (size > BlockSizes[SizeNum - 1])
        ptr = malloc(size);
	else if (THREAD_ALLOC && (warmup_finish || enable_thread_mem_pool)) {
		int arena_id = get_arena_id();
		int size_id = get_size_id(size);
		ptr = _arenas[arena_id][size_id].allocate();
	} else {
		ptr = malloc(size);
	}
	return ptr;
}
