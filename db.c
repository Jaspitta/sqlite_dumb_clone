#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// hold the information needed for calling the getline
typedef struct {
  char* buff;
  size_t buff_len;
  ssize_t in_len;
} InputBuffer;


// allocate the memory needed for an InputBuffer struct and initialize the members
InputBuffer* new_input_buffer(void) {
  InputBuffer* input_buffer = (InputBuffer*) malloc(sizeof(InputBuffer));
  input_buffer->buff = NULL;
  input_buffer->buff_len = 0;
  input_buffer->in_len = 0;

  return input_buffer;
}

void free_input_buffer(InputBuffer* in_buff) {
  free(in_buff->buff);
  free(in_buff);
}

// Read from stdin using the getline function and an InputBuffer to hold the result
// Discard the new line character at the end
void read_input_buffer(InputBuffer* in_buff) {
    in_buff->in_len = getline(&(in_buff->buff), &(in_buff->buff_len), stdin);

    if(in_buff->in_len <= 0) {
      printf("Error reading input line\n");
      free_input_buffer(in_buff);
      exit(1);
    }

    // discard the new line character
    in_buff->in_len -= 1;
    in_buff->buff[in_buff->in_len] = 0;
}

void print_prompt_prefix(void) { printf("db > "); }

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

// check buffer and see if meta command passed is recognized.
// If it is return success if not return unrecognized
MetaCommandResult handle_meta_command(InputBuffer* in_buff) {

  if(in_buff->in_len <= 0) {
    printf("The buffer passed to handle_meta_command is empty");
    exit(1);
  }

  if(strncmp(in_buff->buff, ".exit", 5) == 0) {
    printf("Recognized .exit meta command, exiting program\n");
    exit(0);
  } else {
    return META_COMMAND_UNRECOGNIZED;
  }

}

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT
} StatementType;

typedef struct {
  StatementType type;
} Statement;

// results of preparing a statemt
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED
} PrepareResult;

// check buffer and use it to prepare the stament in the passed statement.
// If the statement is not recognized return an unrecognized
PrepareResult prepare_statement(InputBuffer* in_buff, Statement* statement) {
  if(in_buff->in_len <= 0) {
    printf("Buffer empty when preparing statement\n");
    exit(1);
  }

  if(strncmp(in_buff->buff, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    return PREPARE_SUCCESS;
  } else if(strncmp(in_buff->buff, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED;
}

// execute the passed statement based on it's type
// entry point for back-end
void execute_statement(Statement* statement) {
  switch(statement->type) {
    case (STATEMENT_INSERT):
      printf("executing insert statement\n");
      return;
    case (STATEMENT_SELECT):
      printf("executing select statement\n");
      return;
  }
}


int main (void) {
  printf("Hello, welcome to this small sqlLite clone\n");

  // read-execute-print 
  while(1) {
    print_prompt_prefix();

    InputBuffer* in_buff = new_input_buffer();
    read_input_buffer(in_buff);

    // implemented statements
    if(in_buff->in_len > 0 && in_buff->buff[0] == '.') {
      switch(handle_meta_command(in_buff)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED):
          printf("You entered '%s' but it is not a valid meta command\n", in_buff->buff);
          free_input_buffer(in_buff);
      }
    } else {
      // So this is basically saying, reserve on the stack the memory needed for this struct
      Statement statement;
      switch(prepare_statement(in_buff, &statement)) {
        case (PREPARE_SUCCESS):
          printf("Statement succesfully prepared\n");
          break;
        case (PREPARE_UNRECOGNIZED):
          printf("Unrecognized statement %s\n", in_buff->buff);
          exit(1);
      }

      execute_statement(&statement);
      printf("Statement %s executed \n", in_buff->buff);
    }

  }
  return 0;
}
