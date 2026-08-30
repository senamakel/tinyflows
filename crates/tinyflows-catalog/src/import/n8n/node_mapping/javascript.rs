/// Whether `source` looks like it relies on n8n's code-node runtime globals
/// or return convention rather than tinyflows' stdin/stdout contract — a
/// lightweight lexer. String/comment contents are skipped, and `return` is
/// incompatible only outside a function body.
fn uses_n8n_code_globals(source: &str) -> bool {
    let bytes = source.as_bytes();
    let mut index = 0;
    let mut brace_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut function_depths = Vec::new();
    let mut pending_function_body = false;
    while index < bytes.len() {
        match bytes[index] {
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len()
                    && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            quote @ (b'\'' | b'"') => index = skip_quoted(bytes, index, quote),
            b'`' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'`' {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == b'$' && bytes.get(index + 1) == Some(&b'{') {
                        let start = index + 2;
                        let end = template_expression_end(bytes, start);
                        if uses_n8n_code_globals(&source[start..end]) {
                            return true;
                        }
                        index = (end + 1).min(bytes.len());
                    } else {
                        index += 1;
                    }
                }
                index = (index + 1).min(bytes.len());
            }
            b'=' if bytes.get(index + 1) == Some(&b'>') => {
                pending_function_body = true;
                index += 2;
            }
            b'(' => {
                paren_depth += 1;
                index += 1;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                index += 1;
            }
            b'{' => {
                brace_depth += 1;
                if pending_function_body && paren_depth == 0 {
                    function_depths.push(brace_depth);
                    pending_function_body = false;
                }
                index += 1;
            }
            b'}' => {
                if function_depths.last() == Some(&brace_depth) {
                    function_depths.pop();
                }
                brace_depth = brace_depth.saturating_sub(1);
                index += 1;
            }
            first if first.is_ascii_alphabetic() || matches!(first, b'_' | b'$') => {
                let start = index;
                index += 1;
                while bytes.get(index).is_some_and(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, b'_' | b'$')
                }) {
                    index += 1;
                }
                let token = &source[start..index];
                if token == "function" {
                    pending_function_body = true;
                } else if ["$json", "$input", "$node", "items"].contains(&token)
                    || (token == "return" && function_depths.is_empty())
                {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
}

fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    index += 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            index = (index + 2).min(bytes.len());
        } else if bytes[index] == quote {
            return index + 1;
        } else {
            index += 1;
        }
    }
    index
}

fn template_expression_end(bytes: &[u8], mut index: usize) -> usize {
    let mut depth = 1usize;
    while index < bytes.len() {
        match bytes[index] {
            quote @ (b'\'' | b'"' | b'`') => index = skip_quoted(bytes, index, quote),
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return index;
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    bytes.len()
}
