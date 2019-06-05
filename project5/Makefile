.SUFFIXES: .cpp .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:= $(SRCDIR)main.cpp
TARGET_OBJ:= $(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:= $(wildcard src/*.cpp)
OBJS_FOR_LIB:= $(SRCS_FOR_LIB:.cpp=.o) 

CFLAGS+= -g -std=c++11 -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): 
	$(foreach file,$(SRCS_FOR_LIB),$(CC) $(CFLAGS) -o $(file:.cpp=.o) -c $(file);${\n})
	$(CC) $(CFLAGS) -o $(SRCDIR)main.o -c $(SRCDIR)main.cpp
	make static_library
	$(CC) $(CFLAGS) -o $@ $(SRCDIR)main.o -L $(LIBS) -lbpt -lpthread

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	g++ -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
