#!/usr/bin/env ruby
# frozen_string_literal: true

# One-off repository refactor: move inline #[cfg(test)] modules into sibling
# *_tests.rs files without changing their Rust module names.

def closing_brace(source, opening)
  depth = 0
  index = opening
  state = :normal
  block_depth = 0
  raw_hashes = 0

  while index < source.length
    char = source[index]
    pair = source[index, 2]

    case state
    when :normal
      if pair == "//"
        state = :line_comment
        index += 2
        next
      elsif pair == "/*"
        state = :block_comment
        block_depth = 1
        index += 2
        next
      elsif char == '"'
        state = :string
      elsif char == "'"
        closing = source.index("'", index + 1)
        state = :char if closing && closing - index <= 4
      elsif char == 'r' || (char == 'b' && source[index + 1] == 'r')
        raw_start = index + (char == 'b' ? 2 : 1)
        cursor = raw_start
        cursor += 1 while source[cursor] == '#'
        if source[cursor] == '"'
          raw_hashes = cursor - raw_start
          state = :raw_string
          index = cursor
        end
      elsif char == '{'
        depth += 1
      elsif char == '}'
        depth -= 1
        return index if depth.zero?
      end
    when :line_comment
      state = :normal if char == "\n"
    when :block_comment
      if pair == "/*"
        block_depth += 1
        index += 2
        next
      elsif pair == "*/"
        block_depth -= 1
        state = :normal if block_depth.zero?
        index += 2
        next
      end
    when :string, :char
      if char == '\\'
        index += 2
        next
      elsif (state == :string && char == '"') || (state == :char && char == "'")
        state = :normal
      end
    when :raw_string
      terminator = '"' + ('#' * raw_hashes)
      if source[index, terminator.length] == terminator
        state = :normal
        index += terminator.length
        next
      end
    end

    index += 1
  end

  raise "unclosed module brace at byte #{opening}"
end

Dir.glob('src/**/*.rs').sort.each do |path|
  source = File.read(path)
  matches = []
  pattern = /(?m)^(?<indent>[ \t]*)#\[cfg\(test\)\][ \t]*\n\k<indent>mod[ \t]+(?<name>[A-Za-z0-9_]+)[ \t]*\{/
  cursor = 0

  while (match = pattern.match(source, cursor))
    opening = source.index('{', match.begin(0))
    closing = closing_brace(source, opening)
    matches << [match.begin(0), closing + 1, match[:indent], match[:name], opening, closing]
    cursor = closing + 1
  end

  next if matches.empty?

  stem = File.basename(path, '.rs')
  stem = 'mod' if stem == 'mod'
  replacements = []

  matches.each do |start_at, end_at, indent, name, opening, closing|
    filename = if name.end_with?('_tests')
                 "#{name}.rs"
               elsif name == 'tests' || name == 'test'
                 "#{stem}_tests.rs"
               else
                 "#{name}_tests.rs"
               end
    destination = File.join(File.dirname(path), filename)
    raise "refusing to overwrite #{destination}" if File.exist?(destination)

    body = source[(opening + 1)...closing]
    body = body.sub(/\A\r?\n/, '').sub(/\r?\n[ \t]*\z/, '') + "\n"
    File.write(destination, body)
    declaration = "#{indent}#[cfg(test)]\n#{indent}#[path = \"#{filename}\"]\n#{indent}mod #{name};"
    replacements << [start_at, end_at, declaration]
    warn "#{path}: #{name} -> #{destination}"
  end

  replacements.reverse_each do |start_at, end_at, declaration|
    source[start_at...end_at] = declaration
  end
  File.write(path, source)
end
