CC = gcc
CFLAGS = -std=c11 -O2 -Wall -Wextra -Wpedantic -pthread
LDLIBS = -pthread -lrt
TARGET = campuscompute
SOURCES = main.c banker_custom.c

all: $(TARGET)
$(TARGET): $(SOURCES)
	$(CC) $(CFLAGS) -o $@ $(SOURCES) $(LDLIBS)

check: $(TARGET)
	bash test_commands.sh
clean:
	rm -f $(TARGET)
.PHONY: all check clean
