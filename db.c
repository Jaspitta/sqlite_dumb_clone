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
    //printf("Recognized .exit meta command, exiting program\n");
    exit(0);
  } else {
    return META_COMMAND_UNRECOGNIZED;
  }

}

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  // the +1 is for termination character
  char username[COLUMN_USERNAME_SIZE+1];
  char email[COLUMN_EMAIL_SIZE+1];
} Row;

typedef struct {
  StatementType type;
  Row row_insert; // maybe we could use a union here
} Statement;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// results of preparing a statemt
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED,
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
  PREPARE_NEGATIVE_ID,
} PrepareResult;

PrepareResult validate_insert_statement(InputBuffer* in_buff, Statement* statement) {
    statement->type = STATEMENT_INSERT;

    for(int i = 0, spaces = 0; i < in_buff->in_len; i++){
      while(spaces < 1 && i < in_buff->in_len) {
        if(in_buff->buff[i] == ' ') spaces++;
        i++;
      }

      if(in_buff->buff[i] == '-') {
        printf("ID can not be negative\n");
        return PREPARE_NEGATIVE_ID;
      }

      while(spaces < 2 && i < in_buff->in_len) {
        if(in_buff->buff[i] == ' ') spaces++;
        i++;
      }

      int start_username = i;
      while(spaces == 2 && i < in_buff->in_len) {
        if(in_buff->buff[i] == ' ') spaces++;
        i++;
      }

      if(i - start_username - 1 > COLUMN_USERNAME_SIZE) {
        printf("Input username in insert statement is bigger than the max size of %d\n", COLUMN_USERNAME_SIZE);
        return PREPARE_STRING_TOO_LONG;
      }

      if(in_buff->in_len - i > COLUMN_EMAIL_SIZE) {
        printf("Input email in insert statement is bigger than the max size of %d\n", COLUMN_EMAIL_SIZE);
        return PREPARE_STRING_TOO_LONG;
      }
    }

  return PREPARE_SUCCESS;
}

// check buffer and use it to prepare the stament in the passed statement.
// If the statement is not recognized return an unrecognized
PrepareResult prepare_statement(InputBuffer* in_buff, Statement* statement) {
  if(in_buff->in_len <= 0) {
    printf("Buffer empty when preparing statement\n");
    exit(1);
  }

  if(strncmp(in_buff->buff, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;

    PrepareResult validation = validate_insert_statement(in_buff, statement);
    if(validation != PREPARE_SUCCESS) return validation;


    int args_red = sscanf(in_buff->buff, "insert %d %s %s", &(statement->row_insert.id), ((char*)(statement->row_insert.username)), ((char*)(statement->row_insert.email)));
    if(args_red != 3) {
      printf("Red an erroneous number of arguments to an insert statement");
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  } else if(strncmp(in_buff->buff, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED;
}

// copy the row into the destination memory address contiguously
void serialize_row(Row* source, void* destination) {
  memcpy((char*)destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy((char*)destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy((char*)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// compy the data from the given addres into the passed destination Row struct
void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), (char*)source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), (char*)source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), (char*)source + EMAIL_OFFSET, EMAIL_SIZE);
}

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  // represent the number of the row we are adding/retrieveing.
  // This is kept track of by incrementing every time we add rows
  uint32_t num_rows;
  void* pages[TABLE_MAX_PAGES];
} Table;

Table* new_table(void) {
  Table* table = (Table*)malloc(sizeof(Table));
  table->num_rows = 0;
  for(int i = 0; i < TABLE_MAX_PAGES; i++) table->pages[i] = NULL; 

  return table;
}

void free_table(Table* table) {
  for(int i = 0; i < TABLE_MAX_PAGES; i++) free(table->pages[i]);
  free(table);
}

// given the table and the number of the row
// gets the right page, if null allocate memory for it
// calculate the offset of the row and the bytes in the page
// return the offset in bytes so the start address of the row
//
// given the table it gives the starting address of the next row if you pass
// num_rows or of a specific row if you pass a different number
void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = table->pages[page_num];

  // allocate memory for page that has not been used yet
  if(page == NULL) page = table->pages[page_num] = malloc(PAGE_SIZE);

  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;

  return (char*)page + byte_offset;
}

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL,
} ExecuteResult;

ExecuteResult execute_insert(Statement* statement, Table* table) {
  if(table->num_rows >= TABLE_MAX_ROWS) {
    //printf("Table is already full");
    return EXECUTE_TABLE_FULL;
  }

  Row* row = &(statement->row_insert);
  serialize_row(row, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table* table) {
  Row row;
  for(uint32_t i = 0; i < table->num_rows; i ++) {
    deserialize_row(row_slot(table, i), &(row));
    print_row(&row);
  }

  return EXECUTE_SUCCESS;
}

// execute the passed statement based on it's type
// entry point for back-end
ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch(statement->type) {
    case (STATEMENT_INSERT):
      //printf("executing insert statement\n");
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      //printf("executing select statement\n");
      return execute_select(table);
  }
}


int main (void) {
  //printf("Hello, welcome to this small sqlLite clone\n");
  Table* table = new_table();

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
          //printf("Statement succesfully prepared\n");
          break;
        case (PREPARE_SYNTAX_ERROR):
          printf("Syntax error, could not parse statement\n");
          free_input_buffer(in_buff);
          continue;
        case (PREPARE_STRING_TOO_LONG):
          printf("Syntax error, could not parse statement\n");
          free_input_buffer(in_buff);
          continue;
        case (PREPARE_NEGATIVE_ID):
          printf("Syntax error, could not parse statement\n");
          free_input_buffer(in_buff);
          continue;
        case (PREPARE_UNRECOGNIZED):
          printf("Unrecognized statement %s\n", in_buff->buff);
          exit(1);
      }

      switch(execute_statement(&statement, table)) {
        case (EXECUTE_SUCCESS):
          printf("Executed.\n");
          break;
        case (EXECUTE_TABLE_FULL):
          printf("Error: Table full.\n");
          free_input_buffer(in_buff);
          break;
      }
    }

  }

  free_table(table);
  return 0;
}
