describe 'database' do
  before do 
    `rm -f test.db`
  end

  def run_script(commands)
    raw_output = nil
    IO.popen("./db test.db", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'inserts and retrieves a row' do
    result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ])
    expect(result).to match_array([
      "db > Executed.",
      "db > (1, user1, person1@example.com)",
      "Executed.",
      "db > ",
    ])
  end

  it 'prints error message when table is full' do
    script = (1..1401).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result[-2]).to eq('db > Error: Table full.')
  end

  it 'allows inserting strings that are the maximum length' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > Executed.",
      "db > (1, #{long_username}, #{long_email})",
      "Executed.",
      "db > ",
    ])
  end

  it 'it stops on username over 32' do
    long_username = "a"*33
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > Input username in insert statement is bigger than the max size of 32",
      "Syntax error, could not parse statement",
      "db > ",
    ])
  end

  it 'it stops on email over 255' do
    long_username = "a"*32
    long_email = "a"*256
    script = [
      "insert 1 #{long_username} #{long_email}",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > Input email in insert statement is bigger than the max size of 255",
      "Syntax error, could not parse statement",
      "db > ",
    ])
  end

  it 'it stops on negative id' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert -1 #{long_username} #{long_email}",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ID can not be negative",
      "Syntax error, could not parse statement",
      "db > ",
    ])
  end

  it 'persist data' do
    long_username = "a"*5
    long_email = "a"*5
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > Executed.",
      "db > (1, aaaaa, aaaaa)",
      "Executed.",
      "db > ",
    ])
    script = [
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > (1, aaaaa, aaaaa)",
      "Executed.",
      "db > ",
    ])
  end

end
