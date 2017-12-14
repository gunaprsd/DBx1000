CC=g++
CFLAGS=-Wall -g -std=c++0x

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./ycsb/ ./tpcc/ ./cc/ ./storage/ ./system/ ./workload/
INCLUDE = -I. -I./ycsb -I./tpcc -I./cc -I./storage -I./system -I./lib/include -I./workload

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -O3
LDFLAGS = -Wall -L. -L./lib -pthread -g -lrt -std=c++0x -O3 -ljemalloc -lmetis -lparmetis
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS)
	@ $(CC) -o $@ $^ $(LDFLAGS)
	@echo "Building the executable";

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	@$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	@$(CC) -c $(CFLAGS) -o $@ $<
	@echo "Compiling $<";

.PHONY: clean
clean:
	@rm -f rundb $(OBJS) $(DEPS)
	@echo "Cleaning files";
