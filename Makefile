CC = gcc
CFLAGS = -O2 -Wall -Wextra -Wpedantic -Werror

program_name: db.c
		$(CC) $(CFLAGS) -o db ./db.c 

clean:
		rm -f db

test:
		bundle exec rspec
