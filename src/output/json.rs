//! JSON Renderer
//!
//! JSON output formatting.

#![allow(dead_code)]

use anyhow::Result;

use crate::query::QueryResult;
use crate::storage::Value;

/// JSON renderer
pub struct JsonRenderer {
    pretty: bool,
    colorize: bool,
}

impl JsonRenderer {
    /// Create a new JSON renderer
    pub fn new() -> Self {
        Self {
            pretty: true,
            colorize: true,
        }
    }

    /// Set pretty printing
    pub fn pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }

    /// Set colorization
    pub fn colorize(mut self, colorize: bool) -> Self {
        self.colorize = colorize;
        self
    }

    /// Render query results as JSON
    pub fn render(&self, results: &QueryResult) -> Result<()> {
        let json = results.to_json();

        let output = if self.pretty {
            serde_json::to_string_pretty(&json)?
        } else {
            serde_json::to_string(&json)?
        };

        if self.colorize {
            println!("{}", self.colorize_json(&output));
        } else {
            println!("{}", output);
        }

        Ok(())
    }

    /// Render a Value as JSON
    pub fn render_value(&self, value: &Value) -> Result<()> {
        let json = value.to_json();

        let output = if self.pretty {
            serde_json::to_string_pretty(&json)?
        } else {
            serde_json::to_string(&json)?
        };

        if self.colorize {
            println!("{}", self.colorize_json(&output));
        } else {
            println!("{}", output);
        }

        Ok(())
    }

    /// Add syntax highlighting to JSON
    fn colorize_json(&self, json: &str) -> String {
        let mut result = String::new();
        let mut in_string = false;
        let mut escape_next = false;
        let mut chars = json.chars().peekable();

        while let Some(ch) = chars.next() {
            if escape_next {
                result.push(ch);
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => {
                    result.push(ch);
                    escape_next = true;
                }
                '"' => {
                    if in_string {
                        result.push('"');
                        result.push_str("\x1b[0m"); // Reset color
                        in_string = false;
                    } else {
                        result.push_str("\x1b[32m"); // Green for strings
                        result.push('"');
                        in_string = true;
                    }
                }
                ':' if !in_string => {
                    result.push_str("\x1b[0m"); // Reset
                    result.push(':');
                }
                '{' | '}' | '[' | ']' if !in_string => {
                    result.push_str("\x1b[33m"); // Yellow for brackets
                    result.push(ch);
                    result.push_str("\x1b[0m");
                }
                '0'..='9' | '-' | '.' if !in_string => {
                    result.push_str("\x1b[36m"); // Cyan for numbers
                    result.push(ch);
                    // Continue collecting number
                    while let Some(&next) = chars.peek() {
                        if next.is_ascii_digit()
                            || next == '.'
                            || next == 'e'
                            || next == 'E'
                            || next == '+'
                            || next == '-'
                        {
                            result.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                    result.push_str("\x1b[0m");
                }
                _ => {
                    // Check for keywords
                    if !in_string {
                        let remaining: String =
                            std::iter::once(ch).chain(chars.clone().take(4)).collect();
                        if remaining.starts_with("true")
                            || remaining.starts_with("false")
                            || remaining.starts_with("null")
                        {
                            let keyword = if remaining.starts_with("true") {
                                "true"
                            } else if remaining.starts_with("false") {
                                "false"
                            } else {
                                "null"
                            };
                            result.push_str("\x1b[35m"); // Magenta for keywords
                            result.push_str(keyword);
                            result.push_str("\x1b[0m");
                            for _ in 0..keyword.len() - 1 {
                                chars.next();
                            }
                            continue;
                        }
                    }
                    result.push(ch);
                }
            }
        }

        // Reset at end
        result.push_str("\x1b[0m");
        result
    }
}

impl Default for JsonRenderer {
    fn default() -> Self {
        Self::new()
    }
}
