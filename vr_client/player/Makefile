# compiler
CC = gcc
CFLAGS = -Wall -Wextra -Wpedantic -Wformat-security -Wconversion -Werror -g

# sources and target
MAIN = h1_p2
TARGET = http-1.1_p_pl

# compilation
all:
	$(CC) $(CFLAGS) -o $(TARGET) $(MAIN).c -lcurl -lpthread -lm

clean:
	rm -f $(TARGET)