LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall
O = ./bin

obj = $(O)/kvstore.o $(O)/ssTable.o $(O)/cache.o

all: correctness persistence

correctness: $(O)/correctness.o $(obj)
	g++ $< $(obj) -o correctness

persistence: $(O)/persistence.o $(obj)
	g++ $< $(obj) -o persistence

$(O)/kvstore.o:$(O)/ssTable.o

$(O)/%.o:%.cc
	g++ -c $< -o $@



clean:
	-del -f correctness.exe persistence.exe $(o)/*.o
#	-rm -f correctness persistence $(o)/*.o


