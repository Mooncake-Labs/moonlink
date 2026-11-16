use core::str;
use std::str::Utf8Error;

use thiserror::Error;
use tokio_postgres::types::Type;
use tracing::error;

use crate::pg_replicate::conversions::text::TextFormatConverter;
use crate::pg_replicate::util::normalize_numeric_cell;

use super::Cell;

#[derive(Debug)]
pub struct TableRow {
    pub values: Vec<Cell>,
}

#[derive(Debug, Error)]
pub enum TableRowConversionError {
    #[error("unsupported type {0}")]
    UnsupportedType(Type),

    #[error("invalid string: {0}")]
    InvalidString(#[from] Utf8Error),

    #[error("mismatch in num of columns in schema and row")]
    NumColsMismatch,

    #[error("unterminated row")]
    UnterminatedRow,

    #[error("column {column} ({type_name}): cannot convert {value}: {reason}")]
    InvalidValue {
        column: String,
        type_name: String,
        value: String,
        reason: String,
    },
}

pub struct TableRowConverter;

impl TableRowConverter {
    // parses text produced by this code in Postgres: https://github.com/postgres/postgres/blob/263a3f5f7f508167dbeafc2aefd5835b41d77481/src/backend/commands/copyto.c#L988-L1134
    pub fn try_from(
        row: &[u8],
        column_schemas: &[crate::pg_replicate::table::ColumnSchema],
    ) -> Result<TableRow, TableRowConversionError> {
        let mut values = Vec::with_capacity(column_schemas.len());

        let row_str = str::from_utf8(row)?;
        let bytes = row_str.as_bytes();
        let mut column_schemas_iter = column_schemas.iter();
        // Unescaped values of fields with a backslash; any other field is read in place.
        let mut unescaped = String::new();
        let mut row_terminated = false;
        let mut pos = 0;

        loop {
            // The delimiters are ASCII, so every index found here is a char boundary.
            let start = pos;
            let mut end = start;
            while end < bytes.len() && !matches!(bytes[end], b'\t' | b'\n' | b'\\') {
                end += 1;
            }
            // NULL is the field \N as sent, before unescaping: a value that is backslash-N itself is
            // sent as \\N.
            let (val_str, is_null) = match bytes.get(end) {
                Some(b'\\') => {
                    unescaped.clear();
                    unescaped.push_str(&row_str[start..end]);
                    match Self::unescape_rest(&row_str[end..], &mut unescaped) {
                        Some((consumed, terminator)) => {
                            pos = end + consumed;
                            row_terminated |= terminator == '\n';
                        }
                        None if row_terminated => break,
                        None => return Err(TableRowConversionError::UnterminatedRow),
                    }
                    // Without its one-byte terminator.
                    (unescaped.as_str(), &row_str[start..pos - 1] == "\\N")
                }
                Some(terminator) => {
                    row_terminated |= *terminator == b'\n';
                    pos = end + 1;
                    (&row_str[start..end], false)
                }
                None if row_terminated => break,
                None => return Err(TableRowConversionError::UnterminatedRow),
            };

            let Some(column_schema) = column_schemas_iter.next() else {
                return Err(TableRowConversionError::NumColsMismatch);
            };

            let value = if is_null {
                Cell::Null
            } else {
                match TextFormatConverter::try_from_str(&column_schema.typ, val_str)
                    .map_err(|e| e.to_string())
                    .and_then(|cell| {
                        normalize_numeric_cell(&column_schema.typ, column_schema.modifier, cell)
                    }) {
                    Ok(value) => value,
                    Err(e) => {
                        error!(
                            "error parsing column `{}` of type `{}` from text `{val_str}`",
                            column_schema.name, column_schema.typ
                        );
                        let value = if val_str.chars().count() > 64 {
                            format!("'{}...'", val_str.chars().take(64).collect::<String>())
                        } else {
                            format!("'{val_str}'")
                        };
                        return Err(TableRowConversionError::InvalidValue {
                            column: column_schema.name.clone(),
                            type_name: column_schema.typ.name().to_string(),
                            value,
                            reason: e,
                        });
                    }
                }
            };

            values.push(value);
        }

        // Too few fields would shift every later column's value into the wrong place.
        if column_schemas_iter.next().is_some() {
            return Err(TableRowConversionError::NumColsMismatch);
        }
        Ok(TableRow { values })
    }

    /// Unescape a field from its first backslash on into `out`, up to the tab or newline that ends
    /// it. Returns the bytes read including that terminator, and the terminator; None if the text
    /// ends first.
    fn unescape_rest(rest: &str, out: &mut String) -> Option<(usize, char)> {
        let mut in_escape = false;
        for (i, c) in rest.char_indices() {
            match c {
                c if in_escape => {
                    // As in COPY FROM: any other escaped char (N included) stands for itself.
                    match c {
                        'b' => out.push(8 as char),
                        'f' => out.push(12 as char),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'v' => out.push(11 as char),
                        c => out.push(c),
                    }
                    in_escape = false;
                }
                '\t' | '\n' => return Some((i + 1, c)),
                '\\' => in_escape = true,
                c => out.push(c),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_replicate::table::ColumnSchema;

    /// The row parser that copied every field char by char, with NULL read from the field as sent
    /// (not after unescaping), an escaped N standing for N, and too few fields an error.
    mod reference {
        use super::*;

        pub fn try_from(
            row: &[u8],
            column_schemas: &[crate::pg_replicate::table::ColumnSchema],
        ) -> Result<TableRow, TableRowConversionError> {
            let mut values = Vec::with_capacity(column_schemas.len());

            let row_str = str::from_utf8(row)?;
            let mut column_schemas_iter = column_schemas.iter();
            let mut chars = row_str.chars();
            let mut val_str = String::with_capacity(10);
            let mut raw = String::new();
            let mut in_escape = false;
            let mut row_terminated = false;
            let mut done = false;

            while !done {
                loop {
                    match chars.next() {
                        Some(c) => match c {
                            c if in_escape => {
                                raw.push(c);
                                if c == 'b' {
                                    val_str.push(8 as char);
                                } else if c == 'f' {
                                    val_str.push(12 as char);
                                } else if c == 'n' {
                                    val_str.push('\n');
                                } else if c == 'r' {
                                    val_str.push('\r');
                                } else if c == 't' {
                                    val_str.push('\t');
                                } else if c == 'v' {
                                    val_str.push(11 as char)
                                } else {
                                    val_str.push(c);
                                }
                                in_escape = false;
                            }
                            '\t' => {
                                break;
                            }
                            '\n' => {
                                row_terminated = true;
                                break;
                            }
                            '\\' => {
                                raw.push(c);
                                in_escape = true;
                            }
                            c => {
                                raw.push(c);
                                val_str.push(c);
                            }
                        },
                        None => {
                            if !row_terminated {
                                return Err(TableRowConversionError::UnterminatedRow);
                            }
                            done = true;
                            break;
                        }
                    }
                }

                if !done {
                    let Some(column_schema) = column_schemas_iter.next() else {
                        return Err(TableRowConversionError::NumColsMismatch);
                    };

                    let value = if raw == "\\N" {
                        Cell::Null
                    } else {
                        match TextFormatConverter::try_from_str(&column_schema.typ, &val_str)
                            .map_err(|e| e.to_string())
                            .and_then(|cell| {
                                normalize_numeric_cell(
                                    &column_schema.typ,
                                    column_schema.modifier,
                                    cell,
                                )
                            }) {
                            Ok(value) => value,
                            Err(e) => {
                                error!(
                                    "error parsing column `{}` of type `{}` from text `{val_str}`",
                                    column_schema.name, column_schema.typ
                                );
                                let value = if val_str.chars().count() > 64 {
                                    format!("'{}...'", val_str.chars().take(64).collect::<String>())
                                } else {
                                    format!("'{val_str}'")
                                };
                                return Err(TableRowConversionError::InvalidValue {
                                    column: column_schema.name.clone(),
                                    type_name: column_schema.typ.name().to_string(),
                                    value,
                                    reason: e,
                                });
                            }
                        }
                    };

                    values.push(value);
                    val_str.clear();
                    raw.clear();
                }
            }

            if column_schemas_iter.next().is_some() {
                return Err(TableRowConversionError::NumColsMismatch);
            }
            Ok(TableRow { values })
        }
    }

    #[test]
    fn rows_parse_as_the_char_by_char_parser() {
        let column = |name: &str, typ: Type| ColumnSchema {
            name: name.into(),
            typ,
            modifier: -1,
            nullable: true,
        };
        let layouts = [
            vec![column("a", Type::TEXT)],
            vec![column("a", Type::TEXT), column("b", Type::TEXT)],
            vec![
                column("a", Type::INT4),
                column("b", Type::TEXT),
                column("c", Type::TEXT),
            ],
        ];
        // Every row of up to six of these chars: delimiters, escapes, the null marker's letters,
        // and a multi-byte char.
        let alphabet = ['5', '\t', '\n', '\\', 'N', 'b', 'é'];
        let mut rows = vec![String::new()];
        let mut last = vec![String::new()];
        for _ in 0..6 {
            last = last
                .iter()
                .flat_map(|row| alphabet.iter().map(move |c| format!("{row}{c}")))
                .collect();
            rows.extend(last.iter().cloned());
        }
        for layout in &layouts {
            for row in &rows {
                assert_eq!(
                    format!("{:?}", TableRowConverter::try_from(row.as_bytes(), layout)),
                    format!("{:?}", reference::try_from(row.as_bytes(), layout)),
                    "{row:?}"
                );
            }
        }
    }

    #[test]
    fn parse_basic_row() {
        let schemas = vec![
            ColumnSchema {
                name: "id".into(),
                typ: Type::INT4,
                modifier: 0,
                nullable: false,
            },
            ColumnSchema {
                name: "text".into(),
                typ: Type::TEXT,
                modifier: 0,
                nullable: false,
            },
        ];
        let row = b"42\thello\n";
        let parsed = TableRowConverter::try_from(row, &schemas).unwrap();
        assert_eq!(parsed.values.len(), 2);
        assert!(matches!(parsed.values[0], Cell::I32(42)));
        assert!(matches!(parsed.values[1], Cell::String(ref s) if s == "hello"));
    }

    #[test]
    fn parse_escaped_and_null_values() {
        let schemas = vec![
            ColumnSchema {
                name: "a".into(),
                typ: Type::TEXT,
                modifier: 0,
                nullable: true,
            },
            ColumnSchema {
                name: "b".into(),
                typ: Type::TEXT,
                modifier: 0,
                nullable: true,
            },
        ];
        // first column NULL, second column contains literal "\\N"
        let row = b"\\N\t\\\\\\N\n";
        let parsed = TableRowConverter::try_from(row, &schemas).unwrap();
        assert!(matches!(parsed.values[0], Cell::Null));
        assert!(matches!(parsed.values[1], Cell::String(ref s) if s.starts_with('\\')));
    }

    fn text_columns(count: usize) -> Vec<ColumnSchema> {
        (0..count)
            .map(|i| ColumnSchema {
                name: format!("c{i}"),
                typ: Type::TEXT,
                modifier: -1,
                nullable: true,
            })
            .collect()
    }

    fn texts(row: &[u8], columns: usize) -> Vec<Option<String>> {
        TableRowConverter::try_from(row, &text_columns(columns))
            .unwrap()
            .values
            .into_iter()
            .map(|cell| match cell {
                Cell::Null => None,
                Cell::String(s) => Some(s),
                other => panic!("not text: {other:?}"),
            })
            .collect()
    }

    /// Only the field \N as sent is NULL. COPY sends the value backslash-N as \\N (as it doubles
    /// every backslash in a value), which is that text, not NULL.
    #[test]
    fn only_the_null_marker_as_sent_is_null() {
        let text = |s: &str| Some(s.to_string());
        // NULL, the value \N, and \N inside values.
        assert_eq!(
            texts(b"\\N\t\\\\N\ta\\\\Nb\t\\\\N\\\\N\tx\\\\N\n", 5),
            vec![
                None,
                text("\\N"),
                text("a\\Nb"),
                text("\\N\\N"),
                text("x\\N")
            ]
        );
        // NULL next to escapes in other fields, and as the last field.
        assert_eq!(
            texts(b"a\\tb\t\\N\t\\\\\t\\N\n", 4),
            vec![text("a\tb"), None, text("\\"), None]
        );
        // COPY never sends an escaped N inside a field; like COPY FROM, it reads as N.
        assert_eq!(texts(b"\\Nx\tx\\N\n", 2), vec![text("Nx"), text("xN")]);
    }

    #[test]
    fn too_few_fields_error() {
        for row in [&b"1\n"[..], b"\\N\n", b"a\\tb\n"] {
            assert!(
                matches!(
                    TableRowConverter::try_from(row, &text_columns(2)),
                    Err(TableRowConversionError::NumColsMismatch)
                ),
                "{row:?}"
            );
        }
        // One empty field is one column's value.
        assert_eq!(texts(b"\n", 1), vec![Some(String::new())]);
    }

    #[test]
    fn unterminated_row_error() {
        let schemas = vec![ColumnSchema {
            name: "a".into(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
        }];
        let err = TableRowConverter::try_from(b"1\t2", &schemas);
        assert!(matches!(
            err.unwrap_err(),
            TableRowConversionError::UnterminatedRow
        ));
    }

    #[test]
    fn num_cols_mismatch_error() {
        let schemas = vec![ColumnSchema {
            name: "a".into(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
        }];
        let err = TableRowConverter::try_from(b"1\t2\n", &schemas);
        assert!(matches!(
            err.unwrap_err(),
            TableRowConversionError::NumColsMismatch
        ));
    }
}
