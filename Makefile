LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

obj = kvstore.o ssTable.o cache.o skiplist.o

kvstore.o:ssTable.o skiplist.o
ssTable.o:skiplist.o

%.o:%.cc
	g++ -c $< -o $@

correctness: correctness.o $(obj)
	g++ correctness.o $(obj) -o correctness

persistence: persistence.o $(obj)
	g++ persistence.o $(obj) -o persistence



clean:
	-rm -f correctness persistence *.o

# all: correctness persistence

# obj = kvstore.cc ssTable.cc cache.cc skiplist.cc

# correctness: correctness.cc $(obj)
# 	g++ correctness.cc $(obj) -o correctness

# persistence: persistence.cc $(obj)
# 	g++ persistence.cc $(obj) -o persistence

# clean:
# 	-rm -f correctness persistence *.o

