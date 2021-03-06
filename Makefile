CC=g++
TEST_INCLUDES=-I ./third_party/yaml-cpp/include -I ./third_party/googletest/googletest/include
LD_TEST=-lgtest -L ./third_party/googletest/build/lib/
TEST_LIB_STATIC=./third_party/yaml-cpp/build/*.a ./third_party/googletest/build/lib/*.a ./db_impl/leveldb/build/libleveldb.a 
TEST_SRCS=$(wildcard tests/*.cc)
TEST_BIN =$(TEST_SRCS:.cc=)

CFLAGS=-std=c++11 -g -Wall -pthread  -fgnu-tm
INCLUDES=-I ./
LDFLAGS= -lpthread
SUBDIRS=db core
SUBSRCS=$(wildcard db/*.cc) $(wildcard core/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(OBJECTS) $(EXEC)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@


$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

test: $(TEST_BIN)

$(TEST_BIN): $(TEST_SRCS)
	$(CC) $(CFLAGS) $(TEST_INCLUDES) $^ $(TEST_LIB_STATIC) $(LDFLAGS) $(LD_TEST) -o $@

clean:
	$(RM) $(OBJECTS)
	$(RM) $(EXEC)
	$(RM) $(TEST_BIN)

.PHONY: $(SUBDIRS) $(EXEC)

